package team.dsys.dssearch.shard;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.cluster.ClusterService;
import team.dsys.dssearch.model.Doc;
import team.dsys.dssearch.rpc.CommonRequest;
import team.dsys.dssearch.rpc.CommonResponse;
import team.dsys.dssearch.rpc.GetResponse;
import team.dsys.dssearch.rpc.ShardService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ShardServiceImpl implements ShardService.Iface {

    // 当这个是本地的存储引擎
    // key - docId, val - doc
    private final ConcurrentHashMap<Integer, Doc> kvMap = new ConcurrentHashMap<>();

    @Value("${cluster.main-shard-num}")
    private int mainShardNum;

    @Autowired
    ClusterService clusterService;

    // categories doc by ids into nodes they stored, but the node ids are dynamically chosen ones(round robin)
    // key - node id, val - docs on that node
    // shard id start from 0
    public HashMap<Integer, List<Long>> shardDocIds(List<Long> docIds) {

        HashMap<Integer, List<Long>> shardToIdsMap = new HashMap<>();

        for (Long docId :docIds){
            long shardId = docId % mainShardNum;
            Integer nodeID = clusterService.getNodeByShardId(shardId);
            shardToIdsMap.computeIfAbsent(nodeID, k -> new ArrayList<>()).add(docId);
        }

        return shardToIdsMap;
    }

    public HashMap<Integer, List<Doc>> shardDocs(List<Doc> docList) {

        HashMap<Integer, List<Doc>> shardToIdsMap = new HashMap<>();

        for (Doc doc :docList){
            long shardId = doc.get_id() % mainShardNum;
            Integer nodeID = clusterService.getNodeByShardId(shardId);
            shardToIdsMap.computeIfAbsent(nodeID, k -> new ArrayList<>()).add(doc);
        }

        return shardToIdsMap;
    }


    // todo
    @Override
    public CommonResponse commonReq(CommonRequest req) throws TException {
        return null;
    }

    @Override
    public GetResponse get(int key) throws TException {
        return kvMap.get();
    }

    // todo
    // 只有存在主分片的node会收到这个请求
    @Override
    public CommonResponse store() {
        // 2pc + fault-tolerance(持久化到日志里，也要能恢复)

    }
}
