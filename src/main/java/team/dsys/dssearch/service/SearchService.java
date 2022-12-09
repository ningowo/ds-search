package team.dsys.dssearch.service;

import lombok.SneakyThrows;
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
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.rpc.Doc;
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
    DocService docService;

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchConfig searchConfig;

    SnowflakeIDGenerator generator = new SnowflakeIDGenerator();

    @SneakyThrows
    public List<Doc> search(String query, int size) {
        // 1. search all shards for available doc ids
        List<Pair<Integer, ScoreDoc>> shardIdAndScoreDocList = new ArrayList<>();

        HashMap<Integer, String> shardIdToNodeAddrMap = new HashMap<>();
        shardIdToNodeAddrMap.put(1, "xxx");
        for (Map.Entry<Integer, String> entry : shardIdToNodeAddrMap.entrySet()) {
            int shardId = entry.getKey();
            String nodeAddr = entry.getValue();

            // get doc ids and scores on all shards
            List<ScoreDoc> resultDocIds = storeEngine.queryTopN(query, 2, 1);
//            ShardService.Client client = getClient(nodeAddr);
//            if (client == null) {
//                continue;
//            }
//            client.queryTopN(query, size, shardId);

            for (ScoreDoc doc : resultDocIds) {
                Pair<Integer, ScoreDoc> pair = Pair.of(shardId, doc);
                shardIdAndScoreDocList.add(pair);
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

        // get docs by doc ids
        List<Doc> resultDocs = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : targetSidToDocIdsMap.entrySet()) {
            int shardId = entry.getKey();
            List<Integer> docIdList = entry.getValue();

            List<Doc> docList = storeEngine.getDocList(docIdList, shardId);
            // String nodeAddr = cluster.xxx(shardId)
//            ShardService.Client client = getClient(nodeAddr);
//            if (client == null) {
//                continue;
//            }
//            client.getDocList(docIdList, shardId);
//
            resultDocs.addAll(docList);
        }

        log.info("Get docs by id:");
        for (Doc doc : resultDocs) {
            log.info(doc.toString());
        }

        return resultDocs;
    }

    private ShardService.Client getClient(String nodeAddr) {
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
            e.printStackTrace();
            return null;
        }
    }

    // first send docs to primary shards' nodes, and the primary shard will replicate logs to nodes that replicas exist.
    public boolean store(List<DocVO> docVOLists) throws TException {
        List<Doc> docs = docVOLists.stream().
                map(docVO -> new Doc(docVO.index, docVO.id, docVO.content)).collect(Collectors.toList());

        // generate global unique doc id
        for (Doc doc : docs) {
            doc.setId(generator.generate());
        }

        // 获取集群的状态
        // primary shard's node id
        //Map node to docs eg node 1 (doc 1, doc 2, doc3..)
        // [key = node id, value = [key = shardId, value=List<Doc>>]]
        HashMap<Integer, HashMap<Integer, List<Doc>>> nodeToDocs;

//        // 1. 存本机
//        // 如果当前节点上有doclist所需的主分片，直接存了
//        List<Doc> docListOnCurrentNode = nodeToDocs.get(searchConfig.getNid());
//        if (docListOnCurrentNode != null) {
//            storeEngine.writeDocList(docListOnCurrentNode);
//        }
//
//        // todo
//        // 2. 存远程
//        // 原来是这个 boolean store = docService.store(nodeToDocs);
//        // 转发nodeToDocs到主分片所在节点(RPC调用)
//        Shards shardsOnCurrentNode = ClusterServiceImpl.getShardsOnCurrentNode();
//        boolean hasPrimary = false;
//        for (Shard shard: shardsOnCurrentNode.idToShards.values()) {
//            if (shard.isPrimary()) {
//                shardServiceHandler.store(nodeToDocs);
//            }
//        }

        return false;
    }
//
//    // Sorted docIds
//    private List<Doc> fetchDocs(List<Long> docIds) {
//        HashMap<Integer, List<Long>> nodeToDocIds = shardServiceImpl.shardDocIds(docIds);
//
//        List<Doc> docs = docService.batchGetDocs(nodeToDocIds);
//
//        return docs;
//    }

}
