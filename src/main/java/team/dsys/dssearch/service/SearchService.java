package team.dsys.dssearch.service;

import cluster.external.shard.proto.DataNodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ScoreDoc;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.ClusterServiceManagerImpl;
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.rpc.ScoreAndDocId;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.util.SnowflakeIDGenerator;
import team.dsys.dssearch.vo.DocVO;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SearchService {

    @Autowired
    ShardServiceImpl shardServiceHandler;

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchConfig searchConfig;

    @Autowired
    ClusterServiceManagerImpl clusterService;

    SnowflakeIDGenerator idGenerator = new SnowflakeIDGenerator();

    public List<DocVO> search(String query, int size) {
        log.info("================ [search start] ================");
        log.info("[search start] Start searching：" + query);

        // 1. search all shards for available doc ids
        List<Pair<Integer, ScoreDoc>> shardIdAndScoreDocList = new ArrayList<>();

        HashMap<Integer, DataNodeInfo> shardIdToNodeAddrMap = clusterService.getRandomNodes();
        for (Map.Entry<Integer, DataNodeInfo> entry : shardIdToNodeAddrMap.entrySet()) {
            int shardId = entry.getKey();
            String nodeAddr = entry.getValue().getAddress();
            log.info("-- Search for shard={}, datanode={}", shardId, nodeAddr);

            // get doc ids and scores on all shards
            if (entry.getValue().getDataNodeId() == searchConfig.getNid()) {
                List<ScoreDoc> resultDocIds;
                resultDocIds = storeEngine.queryTopN(query, size, shardId);
                if (resultDocIds == null) {
                    log.info("Got on node {}, nothing found", searchConfig.getNid());
                    continue;
                }
                for (ScoreDoc doc : resultDocIds) {
                    Pair<Integer, ScoreDoc> pair = Pair.of(shardId, new ScoreDoc(doc.doc, doc.score));
                    shardIdAndScoreDocList.add(pair);
                }
                log.info("Got on node {}, result docId(s)={}", searchConfig.getNid(), resultDocIds);
            } else {
                ShardService.Client client = getClient(nodeAddr);
                if (client == null) {
                    continue;
                }

                List<ScoreAndDocId> resultDocIds;
                try {
                    resultDocIds = client.queryTopN(query, size, shardId);
                } catch (TException e) {
                    log.info("Fail to search from shard {}, error {}", shardId, e.getMessage());
                    e.printStackTrace();
                    continue;
                }

                if (resultDocIds == null) {
                    log.info("Got on node {}, nothing found", searchConfig.getNid());
                    continue;
                }
                for (ScoreAndDocId doc : resultDocIds) {
                    Pair<Integer, ScoreDoc> pair = Pair.of(shardId, new ScoreDoc(doc.docId, (float) doc.score));
                    shardIdAndScoreDocList.add(pair);
                }
                log.info("Got on node {}, result docId(s)={}", searchConfig.getNid(), resultDocIds);
            }
        }
        log.info("Get total {} docs by search all nodes", shardIdAndScoreDocList.size());

        // 2. sort all doc ids, and get
        List<Pair<Integer, ScoreDoc>> targetSidToDocIds = shardIdAndScoreDocList.stream()
                .sorted((o1, o2) -> (int) (o1.getRight().score - o2.getRight().score))
                .limit(size)
                .collect(Collectors.toList());

        // build RPC request parameter
        LinkedHashMap<Integer, List<Integer>> targetSidToDocIdsMap = new LinkedHashMap<>();
        for (Pair<Integer, ScoreDoc> pair : targetSidToDocIds) {
            int shardId = pair.getLeft();
            int doc = pair.getRight().doc;
            targetSidToDocIdsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(doc);
        }

        // 3. get docs by doc ids
        List<Doc> resultDocs = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : targetSidToDocIdsMap.entrySet()) {
            int shardId = entry.getKey();
            List<Integer> docIdList = entry.getValue();

            String nodeAddr = clusterService.getRandomNode(shardId).getAddress();
            ShardService.Client client = getClient(nodeAddr);
            if (client == null) {
                continue;
            }
            try {
                resultDocs.addAll(client.getDocList(docIdList, shardId));
            } catch (TException e) {
                log.info("Failed to get docs from shard {}", shardId);
                e.printStackTrace();
            }
        }
        if (resultDocs.size() == 0) {
            log.info("Search for {} finished, no proper doc found", query);
            return Collections.emptyList();
        }

        log.info("Search for {} finished, got {} docs", query, resultDocs.size());
        log.info("================ [search end] ================");

        return resultDocs.stream()
                .map(doc -> new DocVO(doc.index, doc.id, doc.content))
                .collect(Collectors.toList());
    }

    public boolean storeOne(String content) {
        List<String> contents = new ArrayList<>();
        contents.add(content);
        return store(contents);
    }

    public boolean store(List<String> docContents) {
        // 1. build doc objects with docId generated
        List<Doc> docs = buildDocListWithId(docContents);
        log.info("docs to store={}", docs);

        // 2. assign docId to shards
        HashMap<Integer, List<Doc>> shardToDocs = assignDocIdToShards(docs);

        // 3. get shard to node info
        // key - shardId, val - nodeOfPrimaryShard
        HashMap<Integer, DataNodeInfo> nodeOfPrimaryShard =
                clusterService.getNodeOfPrimaryShard(new ArrayList<>(shardToDocs.keySet()));

        // 4. send docs to the node that contains the primary shard of it
        for (Map.Entry<Integer, List<Doc>> entry : shardToDocs.entrySet()) {
            int shardId = entry.getKey();
            List<Doc> shardDocs = entry.getValue();
            int currentNodeId = searchConfig.getNid();

            DataNodeInfo dataNodeInfo = nodeOfPrimaryShard.get(shardId);
            int targetNodeId = dataNodeInfo.getDataNodeId();

            ShardService.Client client = getClient(dataNodeInfo.getAddress());
            try {
                if (client == null) {
                    continue;
                }
                client.transferStoreReq(shardId, shardDocs);
            } catch (TException e) {
                e.printStackTrace();
                log.info("Failed to transfer store to shardId={} on nodeId={}", shardId, targetNodeId);
            }
        }

        return true;
    }

    private List<Doc> buildDocListWithId(List<String> docContents) {
        return docContents.stream()
                .map(content -> new Doc(1, idGenerator.generate(), content))
                .collect(Collectors.toList());
    }

    /**
     * short connection
     * @param nodeAddr
     * @return
     */
    public ShardService.Client getClient(String nodeAddr) {
        String[] s = nodeAddr.split(":");
        String addr = s[0];
        int port = Integer.parseInt(s[1]);
        try {
            TSocket transport  = new TSocket(addr, port);
            transport.setTimeout(10 * 1000);  // 10 seconds timeout
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            return new ShardService.Client(protocol);
        } catch (TTransportException e) {
            log.info("Failed to connect to addr: {}", nodeAddr);
            return null;
        }
    }

    public HashMap<Integer, List<Doc>> assignDocIdToShards(List<Doc> docList) {
        int pShardNum = searchConfig.getPrimaryShardNum();

        HashMap<Integer, List<Doc>> shardToIdsMap = new HashMap<>();

        for (Doc doc: docList){
            int shardId = doc.getId() % pShardNum + 1;
            shardToIdsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(doc);
        }

        return shardToIdsMap;
    }

}
