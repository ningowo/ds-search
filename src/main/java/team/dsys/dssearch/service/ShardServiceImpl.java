package team.dsys.dssearch.service;

import cluster.external.shard.proto.DataNodeInfo;
import lombok.extern.slf4j.Slf4j;
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
import team.dsys.dssearch.model.Shards;
import team.dsys.dssearch.rpc.*;
import team.dsys.dssearch.search.StoreEngine;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ShardServiceImpl implements ShardService.Iface {

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchConfig searchConfig;

    @Autowired
    ClusterServiceManagerImpl clusterService;

    @Override
    public CommonRpcResponse store(int shardId, List<Doc> docs) {
        if (storeEngine.writeDocList(docs, shardId)) {
            log.info("Write {} doc(s) to node {} shard {}", docs.size(), searchConfig.getNid(), shardId);
            return new CommonRpcResponse(true, "ok");
        } else {
            return new CommonRpcResponse(
                    false
                    , String.format("Fail to write docs! nodeId=%d, shardId=%d", searchConfig.getNid(), shardId));
        }
    }

    @Override
    public CommonRpcResponse transferStoreReq(int shardId, List<Doc> docs) throws TException {
        // store on itself
        storeEngine.writeDocList(docs, shardId);

        // transfer to others
        HashMap<Integer, Shards> shardIdToShardsMap = clusterService.getTotalShardIdToShardsMap();
        List<DataNodeInfo> replicaList = shardIdToShardsMap.get(shardId).replicaList;

        int gotNum = 1;
        for (DataNodeInfo nodeInfo: replicaList) {
            ShardService.Client client = getClient(nodeInfo.getAddress());
            if (client.store(shardId, docs).success) {
                gotNum++;
            }
        }

        boolean success = gotNum * 2 >= replicaList.size();
        if (success) {
            return new CommonRpcResponse(true, "ok");
        } else {
            return new CommonRpcResponse(false, "Fail to transfer store request to primary shard node");
        }
    }

    @Override
    public List<ScoreAndDocId> queryTopN(String query, int n, int shardId) {
        log.info("queryTopN received, query={}, n={}, shardId={}", query, n, shardId);

        List<ScoreDoc> scoreDocs = storeEngine.queryTopN(query, n, shardId);

        log.info("Got {} docs in node {}, shard {}", scoreDocs.size(), searchConfig.getNid(), shardId);

        return scoreDocs.stream()
                .map(scoreDoc -> new ScoreAndDocId(scoreDoc.score, scoreDoc.doc))
                .collect(Collectors.toList());
    }

    @Override
    public List<Doc> getDocList(List<Integer> sortedLocalDocIds, int shardId) {
        return storeEngine.getDocList(sortedLocalDocIds, shardId);
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

}
