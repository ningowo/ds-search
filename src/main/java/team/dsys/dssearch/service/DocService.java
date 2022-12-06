package team.dsys.dssearch.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import team.dsys.dssearch.cluster.ClusterServiceImpl;
import team.dsys.dssearch.routing.*;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.rpc.GetResponse;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.util.JsonUtil;
import team.dsys.dssearch.util.TransportUtil;
import team.dsys.dssearch.shard.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class DocService {

    private static int RETRY_CNT = 2;

    // key - nodeId, val - connection
    public static final HashMap<Integer, Connection> conns = new HashMap<>();

    /**
     * Manage the connection that the current server used to connect to other servers.
     * Will retry using retry method until fail for certain times.
     * @param routings
     * @return
     */
    public boolean updateConnection(List<NodeRouting> routings) {
        for (NodeRouting routing : routings) {
            int nodeId = routing.getNodeId();
            String host = routing.getHost();
            int port = routing.getPort();

            boolean allSet = false;
            int retryTimes = 0;
            do {
                try {
                    // If this method return successfully, then it means the connection built.
                    conns.put(nodeId, TransportUtil.buildShardConn(nodeId, host, port, 2000));
                    allSet = true;
                } catch (Exception e) {
                    log.info(String.format("Retry connecting to %d...", nodeId));
                    if (retryConnect(routing)) {
                        allSet = true;
                    }
                }
            } while (!allSet && retryTimes++ < RETRY_CNT);
        }

        log.info("Connection all set!");

        return true;
    }

    public Doc getDoc(Integer nodeId, Integer docId) {
        ShardService.Client client = conns.get(nodeId).client;
        try {
            GetResponse resp = client.get(docId);
            if (resp.success) {
//                return JsonUtil.jsonToObject(resp.doc, Doc.class);
                return null;

            }
        } catch (TException e) {
            log.info("Get request toward {} failed: key={}", nodeId, docId);
            e.printStackTrace();
        }
        return null;
    }

    // multi-thread can be used later
    public List<Doc> batchGetDocs(HashMap<Integer, List<Long>> nodesAndDocIds) {
        // refer to the getDoc method above

        return null;
    }

    public boolean store(HashMap<Integer, List<Doc>> nodeIdToDocs) throws TException {
        // todo
        Shards shardsOnCurrentNode = ClusterServiceImpl.getShardsOnCurrentNode();
        boolean hasPrimary = false;
        for (Shard shard: shardsOnCurrentNode.idToShards.values()) {
            if (shard.isPrimary()) {
                ShardServiceImpl shardServiceHandler = new ShardServiceImpl(new ArrayList<>(Arrays.asList(6601, 6602, 6603, 6604)));
                shardServiceHandler.store((List)nodeIdToDocs.values());
            }
        }

        return false;
    }

    /**
     *
     * @return true for successful connection or considered failure.
     */
    private boolean retryConnect(NodeRouting routing) {
        return true;
    }

    public static class Connection {

        public int pid;

        public String hostName;

        public int port;

        public ShardService.Client client;

        public Connection(int pid, String hostName, int port, ShardService.Client client) {
            this.pid = pid;
            this.hostName = hostName;
            this.port = port;
            this.client = client;
        }
    }

}
