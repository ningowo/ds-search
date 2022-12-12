package team.dsys.dssearch.shard;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.cluster.ClusterService;
import team.dsys.dssearch.cluster.Node;
import team.dsys.dssearch.internal.common.ClusterServiceManager;
import team.dsys.dssearch.internal.common.impl.ClusterServiceManagerImpl;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.routing.RoutingTable;
import team.dsys.dssearch.rpc.CommonRequest;
import team.dsys.dssearch.rpc.CommonResponse;
import team.dsys.dssearch.rpc.GetResponse;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.rpc.Transaction;
import team.dsys.dssearch.cluster.ClusterState;
import team.dsys.dssearch.util.FileUtil;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ShardServiceImpl implements ShardService.Iface {
    //only for test
    private final List<Integer> serverPorts;

    @Value("${search.nodeId}")
    private int nodeId;

    private final String CONFIG_FILE_PATH = "../../../resources/cluster.conf";
    private final ClusterServiceManager clusterServiceManager = new ClusterServiceManagerImpl(nodeId, CONFIG_FILE_PATH);

    public ShardServiceImpl(List<Integer> ports) throws TimeoutException {
        serverPorts = ports;
    }

    // 当这个是本地的存储引擎
    // key - docId, val - doc
    private final ConcurrentHashMap<Integer, Doc> kvMap = new ConcurrentHashMap<>();
    private static final String LOG_PATH ="./logs/";
    private static final AtomicInteger transIdCounter = new AtomicInteger(1000);

    @Value("${cluster.main-shard-num}")
    private int mainShardNum;

    @Autowired
    ClusterService clusterService;

    // categories doc by ids into nodes they stored, but the node ids are dynamically chosen ones(round robin)
    // key - node id, val - docs on that node
    // shard id start from 0
    public HashMap<Integer, List<Long>> shardDocIds(List<Long> docIds) {

        HashMap<Integer, List<Long>> shardToIdsMap = new HashMap<>();

        for (Long docId : docIds){
            long shardId = docId % mainShardNum;
            Integer nodeID = clusterService.getNodeByShardId(shardId);
            shardToIdsMap.computeIfAbsent(nodeID, k -> new ArrayList<>()).add(docId);
        }

        return shardToIdsMap;
    }

    public HashMap<Integer, List<Doc>> shardDocs(List<Doc> docList) {

        HashMap<Integer, List<Doc>> shardToIdsMap = new HashMap<>();

        for (Doc doc : docList){
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
        if (kvMap.containsKey(key)) {
            return new GetResponse(true, key, kvMap.get(key));
        }
        return new GetResponse(false, key, null);
    }

    @Override
    public GetResponse batchGet(List<String> docIds) throws TException {
        return null;
    }

    // todo
    // 只有存在主分片的node会收到这个请求
    //Input:
    //Output:
    @Override
    public CommonResponse store(List<Doc> docs) throws TException {
        // 2pc + fault-tolerance(持久化到日志里，也要能恢复)
        //given a list of doc, we store them into node by using 2pc
        //对于所有node做一次2pc,

        List<DataNodeInfo> nodes = null;
        ShardResponse shardResponse = clusterServiceManager.getShardInfo(GetShardRequest.newBuilder().addShardId(shardId));
        for (ShardInfoWithDataNodeInfo shardInfoWithDataNodeInfo : shardResponse.getShardInfoWithDataNodeInfo()) {
            nodes = shardInfoWithDataNodeInfo.getDataNodeInfos();
        }
        for (Doc doc : docs) {
            Transaction trans = new Transaction(generateTransId(), doc.get_id(), doc);
            List<DataNodeInfo> committedNodes = new ArrayList<>();
            for (DataNodeInfo node : nodes) {
                // Two-phase commit
                // First phase: Prepare
                boolean prepareOk = false;
                try {
                    TSocket transport  = new TSocket(node.address);
                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
                    transport.open();
                    //Client call RPC
                    TProtocol protocol = new TBinaryProtocol(transport);
                    ShardService.Client client = new ShardService.Client(protocol);
                    prepareOk = client.prepare(trans);
                    transport.close();
                } catch (TTransportException e) {
                    e.printStackTrace();
                }

                if (!prepareOk) {
                    return new CommonResponse(prepareOk, "abort");
                }
            }

            boolean res = false;
            for (DataNodeInfo node : nodes) {
                boolean each = false;
                // Phase two: commit
                try {
                    TSocket transport = new TSocket(node.address);
                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
                    transport.open();
                    //Client call RPC
                    TProtocol protocol = new TBinaryProtocol(transport);
                    ShardService.Client client = new ShardService.Client(protocol);
                    each = client.commit(trans);
                } catch (TTransportException e) {
                    e.printStackTrace();
                }
                res = each;
                committedNodes.add(node);
                if (!res) {
                    //rollback doc in all previous committed node
                    abort(doc, committedNodes, trans);
                }
            }
            if (!res) {
                return new CommonResponse(res, "abort");
            }
        }
        return new CommonResponse(true, "commit");
    }

    //prepare stage
    @Override
    public boolean prepare(Transaction trans) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        FileUtil.addTransFromMap(LOG_PATH, ts.toString(), trans);
        return true;
    }

    //Add doc to local kvMap
    @Override
    public boolean commit(Transaction trans) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        kvMap.put(trans.key, trans.val);
        FileUtil.removeTransFromMap(LOG_PATH, ts.toString(), trans);
        return true;
    }

    /**
     * remove doc in from node
     * @param trans
     * @return
     */
    @Override
    public boolean remove(Transaction trans) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        FileUtil.removeTransFromMap(LOG_PATH, ts.toString(), trans);
        kvMap.remove(trans.key);
        return true;
    }

    /**
     * rollback all prev operation to the committed node
     * @param doc
     * @param committedNodes
     * @param trans
     * @return
     */
    public boolean abort(Doc doc, List<DataNodeInfo> committedNodes, Transaction trans) {
        for (DataNodeInfo node: committedNodes) {
            try {
                TSocket transport = new TSocket(node.address);
                transport.setTimeout(10 * 1000);  // 10 seconds timeout
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ShardService.Client client = new ShardService.Client(protocol);
                client.remove(trans);
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private int generateTransId() {
        return transIdCounter.getAndIncrement();
    }
}
