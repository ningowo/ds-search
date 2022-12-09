package team.dsys.dssearch.service;

import org.apache.lucene.search.ScoreDoc;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.cluster.ClusterService;
import team.dsys.dssearch.rpc.*;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.util.FileUtil;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class ShardServiceImpl implements ShardService.Iface {

    @Autowired
    StoreEngine storeEngine;

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
            long shardId = doc.getId() % mainShardNum;
            Integer nodeID = clusterService.getNodeByShardId(shardId);
            shardToIdsMap.computeIfAbsent(nodeID, k -> new ArrayList<>()).add(doc);
        }

        return shardToIdsMap;
    }

    @Override
    public List<ScoreAndDocId> queryTopN(String query, int n, int shardId) {
        List<ScoreDoc> scoreDocs = storeEngine.queryTopN(query, n, shardId);
        return scoreDocs.stream()
                .map(scoreDoc -> new ScoreAndDocId(scoreDoc.score, scoreDoc.doc))
                .collect(Collectors.toList());
    }

    @Override
    public List<Doc> getDocList(List<Integer> sortedLocalDocIds, int shardId) {
        return storeEngine.getDocList(sortedLocalDocIds, shardId);
    }

    // todo
    @Override
    public CommonResponse commonReq(CommonRequest req) throws TException {
        return null;
    }

    @Override
    public GetResponse get(int key) throws TException {
//        if (kvMap.containsKey(key)) {
//            return new GetResponse(true, key, kvMap.get(key));
//        }
//        return new GetResponse(false, key, null);
        return null;
    }

    @Override
    public GetResponse batchGet(List<String> docIds) throws TException {
        return null;
    }

    @Override
    public CommonResponse store(List<Doc> docs) throws TException {
        return null;
    }

    // todo
    // 只有存在主分片的node会收到这个请求
    //Input:
    //Output:
//    @Override
    public CommonResponse store(HashMap<Integer, List<Doc>> docs) throws TException {
        for (Map.Entry<Integer, List<Doc>> nodeIdToDocs : docs.entrySet()) {
            Integer nodeId = nodeIdToDocs.getKey();
            List<Doc> docsOnNode = nodeIdToDocs.getValue();

            // RPC, 发到nodeId上

        }


//
//        RoutingTable routingTable = new RoutingTable(); //  placeholder  HashMap<NodeRouting, team.dsys.dssearch.cluster.Node>
//        //NodeRouting:  int nodeId; String host; Integer port;
//        //Node: public String addr; public int port; ShardRoutingTable routingTable;
//        for (Doc doc : docs) {
//            Transaction trans = new Transaction(generateTransId(), doc.get_id(), doc);
//            List<Node> nodes = (List<Node>) routingTable.getNodeAddrs().values();
//            List<Node> committedNodes = new ArrayList<>();
//            for (Node node : nodes) {
//                // Two-phase commit
//                // First phase: Prepare
//                boolean prepareOk = false;
//                try {
//                    TSocket transport  = new TSocket(node.addr, node.port);
//                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
//                    transport.open();
//                    //Client call RPC
//                    TProtocol protocol = new TBinaryProtocol(transport);
//                    ShardService.Client client = new ShardService.Client(protocol);
//                    prepareOk = client.prepare(trans);
//                    transport.close();
//                } catch (TTransportException e) {
//                    e.printStackTrace();
//                }
//
//                if (!prepareOk) {
//                    return new CommonResponse(prepareOk, "abort");
//                }
//            }
//
//            boolean res = false;
//            for (Node node : nodes) {
//                boolean each = false;
//                // Phase two: commit
//                try {
//                    TSocket transport = new TSocket(node.addr, node.port);
//                    transport.setTimeout(10 * 1000);  // 10 seconds timeout
//                    transport.open();
//                    //Client call RPC
//                    TProtocol protocol = new TBinaryProtocol(transport);
//                    ShardService.Client client = new ShardService.Client(protocol);
//                    each = client.commit(trans);
//                } catch (TTransportException e) {
//                    e.printStackTrace();
//                }
//                res = each;
//                committedNodes.add(node);
//                if (!res) {
//                    //rollback doc in all previous committed node
//                    abort(doc, committedNodes, trans);
//                }
//            }
//            if (!res) {
//                return new CommonResponse(res, "abort");
//            }
//        }
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
//        kvMap.put(trans.key, trans.val);
//        FileUtil.removeTransFromMap(LOG_PATH, ts.toString(), trans);
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
//        kvMap.remove(trans.key);
        return true;
    }
//
//    /**
//     * rollback all prev operation to the committed node
//     * @param doc
//     * @param committedNodes
//     * @param trans
//     * @return
//     */
//    public boolean abort(Doc doc, List<Node> committedNodes, Transaction trans) {
//        for (Node node: committedNodes) {
//            try {
//                TSocket transport = new TSocket(node.addr, node.port);
//                transport.setTimeout(10 * 1000);  // 10 seconds timeout
//                transport.open();
//                TProtocol protocol = new TBinaryProtocol(transport);
//                ShardService.Client client = new ShardService.Client(protocol);
//                client.remove(trans);
//            } catch (TTransportException e) {
//                e.printStackTrace();
//            } catch (TException e) {
//                e.printStackTrace();
//            }
//        }
//        return true;
//    }

    private int generateTransId() {
        return transIdCounter.getAndIncrement();
    }
}
