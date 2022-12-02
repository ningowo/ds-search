package team.dsys.dssearch.shard;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.cluster.ClusterService;
import team.dsys.dssearch.cluster.Node;
import team.dsys.dssearch.model.Doc;
import team.dsys.dssearch.routing.RoutingTable;
import team.dsys.dssearch.rpc.CommonRequest;
import team.dsys.dssearch.rpc.CommonResponse;
import team.dsys.dssearch.rpc.GetResponse;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.cluster.ClusterState;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ShardServiceImpl implements ShardService.Iface {

    // 当这个是本地的存储引擎
    // key - docId, val - doc
    private final ConcurrentHashMap<Long, Doc> kvMap = new ConcurrentHashMap<>();

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
    //Input:
    //Output:
    @Override
    public CommonResponse store(List<Doc> docs) {
        // 2pc + fault-tolerance(持久化到日志里，也要能恢复)
        //given a list of doc, we store them into node by using 2pc
        //对于所有node做一次2pc,

        RoutingTable routingTable = new RoutingTable(); //  placeholder  HashMap<NodeRouting, team.dsys.dssearch.cluster.Node>
        //NodeRouting:  int nodeId; String host; Integer port;
        //Node: public String addr; public int port; ShardRoutingTable routingTable;
        for (Doc doc : docs) {
            List<Node> nodes = (List<Node>) routingTable.getNodeAddrs().values();
            List<Node> committedNodes = new ArrayList<>();
            for (Node node : nodes) {
                // Two-phase commit
                // First phase: Prepare
                boolean prepareOk = false;
                try {
                    TSocket transport  = new TSocket(node.addr, node.port);
                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
                    transport.open();
                    //Client call RPC
                    TProtocol protocol = new TBinaryProtocol(transport);
                    ShardService.Client client = new ShardService.Client(protocol);
                    prepareOk = client.prepare(doc.get_id(), doc);
                    transport.close();
                } catch (TTransportException e) {
                    e.printStackTrace();
                }

                if (!prepareOk) {
                    return new CommonResponse(prepareOk, "abort");
                }
            }

            boolean res = false;
            for (Node node : nodes) {
                boolean each = false;
                // Phase two: commit
                try {
                    TSocket transport = new TSocket(node.addr, node.port);
                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
                    transport.open();
                    //Client call RPC
                    TProtocol protocol = new TBinaryProtocol(transport);
                    ShardService.Client client = new ShardService.Client(protocol);
                    each = client.commit(doc.get_id(), doc);
                } catch (TTransportException e) {
                    e.printStackTrace();
                }
                res = each;
                committedNodes.add(node);
                if (!res) {
                    //rollback doc in all previous committed node
                    abort(doc, committedNodes);
                }
            }
            if (res) {
                return new CommonResponse(res, "commit");
            } else {
                return new CommonResponse(res, "abort");
            }
        }
        return new CommonResponse(true, "commit");
    }

    //prepare stage
    public boolean prepare(Doc doc) {
        //TODO store trans info in the log
        return true;
    }

    //Add doc to local kvMap
    public boolean commit(Doc doc) {
        kvMap.put(doc.get_id(), doc);

        //TODO removeTransFromMap
        return true;
    }

    //rollback
    public boolean abort(Doc doc, List<Node> committedNodes) {
        //TODO removeTransFromMap

        for (Node node: committedNodes) {
            kvMap.remove(doc.get_id(), doc);
        }
        return true;
    }
}
