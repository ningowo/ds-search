package team.dsys.dssearch;

import cluster.external.listener.proto.ClusterEndpointsInfo;
import cluster.external.listener.proto.ClusterEndpointsRequest;
import cluster.external.listener.proto.ClusterEndpointsResponse;
import cluster.external.listener.proto.ClusterListenServiceGrpc;
import cluster.external.shard.proto.ShardResponse;
import cluster.external.shard.proto.GetAllShardRequest;
import cluster.external.shard.proto.PutShardRequest;
import cluster.external.shard.proto.GetShardRequest;
import cluster.external.shard.proto.ShardRequestHandlerGrpc;
import cluster.external.shard.proto.ShardInfo;
import cluster.external.shard.proto.DataNodeInfo;
import cluster.external.shard.proto.ShardInfoWithDataNodeInfo;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.internal.common.ClusterServiceManager;
import team.dsys.dssearch.internal.common.config.ClusterServerCommonConfig;
import team.dsys.dssearch.model.Shards;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.getRootCause;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
public class ClusterServiceManagerImpl implements ClusterServiceManager {

    String configFilePath = "src/main/resources/cluster.conf";

    @Autowired
    SearchConfig searchConfig;

    Integer dataNodeId;

    String currentNodeAddr;

    volatile boolean clusterServerStubCreated = false;

    private ClusterServerCommonConfig config;
    private Map<String, String> clusterServerAddress = new LinkedHashMap<>();
    private final AtomicReference<ClusterEndpointsInfo> clusterEndpointsInfoCache = new AtomicReference<>(); //update the value atomically(e.g., thread-safe)
    private volatile LeaderStub leaderStub;
    private static final long CREATE_STUB_TIME_LIMIT = SECONDS.toMillis(60);
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    /**
     * send adding node message to cluster, and get complete shards info
     * @param shardInfoList
     * @return
     */
    public boolean sendShardInfoToCluster(List<ShardInfo> shardInfoList) {
        prepareStub();

        PutShardRequest req = PutShardRequest.newBuilder().
                setDataNodeInfo(DataNodeInfo.newBuilder()
                        .setDataNodeId(searchConfig.getNid())
                        .setAddress(currentNodeAddr)
                        .build())
                .addAllShardInfo(shardInfoList).build();

        ShardResponse shardResponse = putShardInfo(req);

        // 0 - connected, -1 - error
        return shardResponse.getCommonResponse().getStatus() == 0;
    }

    /**
     * get nodes for all shards, so that can search all nodes for query word
     * @return
     */
    public HashMap<Integer, DataNodeInfo> getRandomNodes() {
        List<ShardInfoWithDataNodeInfo> nodeInfos = searchNodeByAllShardId();
        return randomAdaptor(nodeInfos);
    }

    /**
     * pick random nodes for shards to read. key=shardId, val=randomNode
     * @param shardIdList
     * @return
     */
    public HashMap<Integer, DataNodeInfo> geRandomNodes(List<Integer> shardIdList) {
        List<ShardInfoWithDataNodeInfo> nodeInfos = searchNodeByShardId(shardIdList);
        return randomAdaptor(nodeInfos);
    }

    /**
     * pick a random node to read
     * @param shardId
     * @return
     */
    public DataNodeInfo getRandomNode(Integer shardId) {
        List<Integer> one = new ArrayList<>();
        one.add(shardId);
        HashMap<Integer, DataNodeInfo> map = geRandomNodes(one);

        return map.entrySet().iterator().next().getValue();
    }

    private HashMap<Integer, DataNodeInfo> randomAdaptor(List<ShardInfoWithDataNodeInfo> nodeInfos) {
        // randomly pick a node to get
        HashMap<Integer, List<DataNodeInfo>> shardIdToNodeMap = new HashMap<>();
        for (ShardInfoWithDataNodeInfo info : nodeInfos) {
            int shardId = info.getShardInfo().getShardId();
            // designed to ensure one and only one element in the list
            DataNodeInfo nodeInfo = info.getDataNodeInfosList().get(0);

            shardIdToNodeMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(nodeInfo);
        }

        HashMap<Integer, DataNodeInfo> shardIdToChosenNodeMap = new HashMap<>();
        for (Map.Entry<Integer, List<DataNodeInfo>> entry : shardIdToNodeMap.entrySet()) {
            int shardId = entry.getKey();
            List<DataNodeInfo> nodeInfoList = entry.getValue();
            int randomIndex = new Random().nextInt(nodeInfoList.size());

            shardIdToChosenNodeMap.put(shardId, nodeInfoList.get(randomIndex));
        }

        return shardIdToChosenNodeMap;
    }

    /**
     * get nodes that contains the primary key for shards
     * @param shardIdList
     * @return
     */
    public HashMap<Integer, DataNodeInfo> getNodeOfPrimaryShard(List<Integer> shardIdList) {
        List<ShardInfoWithDataNodeInfo> nodeInfos = searchNodeByShardId(shardIdList);

        // pick a node that contains primary key
        HashMap<Integer, DataNodeInfo> shardIdToNodeMap = new HashMap<>();
        for (ShardInfoWithDataNodeInfo info: nodeInfos) {
            if (info.getShardInfo().getIsPrimary()) {
                int shardId = info.getShardInfo().getShardId();
                DataNodeInfo nodeInfo = info.getDataNodeInfos(0);

                shardIdToNodeMap.put(shardId, nodeInfo);
            }
        }

        log.info("Test primary shard, {}", shardIdToNodeMap);

        return shardIdToNodeMap;
    }

    public HashMap<Integer, Shards> getTotalNodeToShardsMap () {
        List<ShardInfoWithDataNodeInfo> nodeInfos = searchNodeByAllShardId();

        HashMap<Integer, Shards> shardIdToNodeMap = new HashMap<>();
        for (ShardInfoWithDataNodeInfo info : nodeInfos) {
            ShardInfo shardInfo = info.getShardInfo();
            int shardId = info.getShardInfo().getShardId();

            // designed to ensure one and only one element in the list
            DataNodeInfo nodeInfo = info.getDataNodeInfosList().get(0);
            int nodeId = nodeInfo.getDataNodeId();

            if (shardInfo.getIsPrimary()) {
                shardIdToNodeMap
                        .computeIfAbsent(nodeId, k -> new Shards(shardId, nodeInfo, new ArrayList<>()))
                        .setPrimary(nodeInfo);
            } else {
                shardIdToNodeMap
                        .computeIfAbsent(nodeId, k -> new Shards(shardId, null, new ArrayList<>()))
                        .replicaList.add(nodeInfo);
            }
        }

        return shardIdToNodeMap;
    }

    /**
     * Get all nodes info.
     * @return
     */
    public HashMap<Integer, Shards> getTotalShardIdToShardsMap() {
        List<ShardInfoWithDataNodeInfo> nodeInfos = searchNodeByAllShardId();

        HashMap<Integer, Shards> shardIdToNodeMap = new HashMap<>();
        for (ShardInfoWithDataNodeInfo info : nodeInfos) {
            ShardInfo shardInfo = info.getShardInfo();
            int shardId = info.getShardInfo().getShardId();

            // designed to ensure one and only one element in the list
            DataNodeInfo nodeInfo = info.getDataNodeInfosList().get(0);

            if (shardInfo.getIsPrimary()) {
                shardIdToNodeMap
                        .computeIfAbsent(shardId, k -> new Shards(shardId, nodeInfo, new ArrayList<>()))
                        .setPrimary(nodeInfo);
            } else {
                shardIdToNodeMap
                        .computeIfAbsent(shardId, k -> new Shards(shardId, null, new ArrayList<>()))
                        .replicaList.add(nodeInfo);
            }
        }

        return shardIdToNodeMap;
    }

    /**
     *
     * @param shardIdList
     * @return complete shard to nodes info
     */
    private List<ShardInfoWithDataNodeInfo> searchNodeByShardId(List<Integer> shardIdList) {
        GetShardRequest req = GetShardRequest.newBuilder()
                .addAllShardId(shardIdList)
                .setMinCommitIndex(-1L)
                .build();
        ShardResponse response = getShardInfo(req);
        return response.getGetShardResponse().getShardInfoWithDataNodeInfoList();
    }

    /**
     * find all shard with node info
     */
    private List<ShardInfoWithDataNodeInfo> searchNodeByAllShardId() {
        ShardResponse shardReport = getShardReport(GetAllShardRequest.newBuilder().build());
        return shardReport.getGetShardResponse().getShardInfoWithDataNodeInfoList();
    }

    private void prepareStub() {
        // current node id
        dataNodeId = searchConfig.getNid();
        currentNodeAddr = searchConfig.getHost() + ":" + searchConfig.getPort();

        // cluster config
        config = ClusterServerCommonConfig.getClusterCommonConfig(configFilePath);
        clusterServerAddress = config.getClusterServerAddress();

        // create stub
        if (!clusterServerStubCreated) {
            try {
                createClusterServerStub();
            } catch (TimeoutException e) {
                log.info("Failed to create cluster server stub, msg={}", e.getMessage());
            }
        }
    }

    private class LeaderStub {
        private String leaderId;
        private cluster.external.shard.proto.ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub;

        LeaderStub(String leaderId, cluster.external.shard.proto.ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub) {
            this.leaderId = leaderId;
            this.stub = stub;
        }
    }

    private void createClusterServerStub() throws TimeoutException {
        //todo: randomly pick, if one failed, try another
        //randomly pick an address from server address
        Object[] values = clusterServerAddress.values().toArray();
        String randomAddress = ((String) values[new Random().nextInt(values.length)]);
        ManagedChannel randomConnectedServerChannel = ManagedChannelBuilder.forTarget(randomAddress).disableRetry().directExecutor().usePlaintext().build();
        ClusterListenServiceGrpc.ClusterListenServiceStub stub = ClusterListenServiceGrpc.newStub(randomConnectedServerChannel);
        log.info("DataNode {} created the cluster server stub for address: {} ", dataNodeId, randomAddress);

        ClusterEndpointsRequest request = ClusterEndpointsRequest.newBuilder().setClientId(dataNodeId.toString()).build();
        long startTime = System.currentTimeMillis();

        try {
            stub.listenClusterEndpoints(request, new ClusterEndpointsResponseObserver(randomAddress));

            while (clusterEndpointsInfoCache.get() == null) {
                if (System.currentTimeMillis() - startTime > CREATE_STUB_TIME_LIMIT) {
                    log.error("Failed to connect to server {} because of TimeOut Exception", randomAddress);
                    throw new TimeoutException("Connecting to server timeout..");
                }

            }
        } finally {
            try {
                randomConnectedServerChannel.shutdown().awaitTermination(1, SECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted exception during gRPC channel close", e);
            }
        }

    }

    private class ClusterEndpointsResponseObserver implements StreamObserver<ClusterEndpointsResponse> {
        String address;

        ClusterEndpointsResponseObserver(String address) {
            this.address = address;
        }

        @Override
        public void onNext(ClusterEndpointsResponse response) {
            ClusterEndpointsInfo endpointsInfo = response.getEndpointsInfo();
            try {
                log.info("DataNode {} received response from cluster server: {}", dataNodeId, address);
                tryUpdateClusterEndpointsInfo(endpointsInfo);
            } catch(Exception e) {
                log.error("Response from address: {} failed", address);
            }
        }

        @Override
        public void onError(Throwable t) {
            System.out.println(t.getMessage());
            log.error("For DataNode {}, Cluster Observer of {} failed. Message: ", dataNodeId, address, t.getMessage());

        }

        @Override
        public void onCompleted() {
            log.info("For DataNode {}, Cluster Observer of {} completed ", dataNodeId, address);
        }
    }

    private void tryUpdateClusterEndpointsInfo(ClusterEndpointsInfo updatedEndpointsInfo) {
        ClusterEndpointsInfo currentEndpointsInfo = clusterEndpointsInfoCache.get();
        if (currentEndpointsInfo == null || currentEndpointsInfo.getTerm() < updatedEndpointsInfo.getTerm()
                || currentEndpointsInfo.getEndpointsCount() < updatedEndpointsInfo.getEndpointsCount()
                || currentEndpointsInfo.getEndpointsCommitIndex() < updatedEndpointsInfo.getEndpointsCommitIndex()
                || !currentEndpointsInfo.getLeaderId().equals(updatedEndpointsInfo.getLeaderId())) {

            log.info("DataNode {} updated cluster endpoints info... ", dataNodeId);
            clusterEndpointsInfoCache.set(updatedEndpointsInfo);
            tryUpdateLeaderInfo(updatedEndpointsInfo);
        }

        return;
    }

    private void tryUpdateLeaderInfo(ClusterEndpointsInfo updatedEndpointsInfo) {
        if (!isNullOrEmpty(updatedEndpointsInfo.getLeaderId()) && (leaderStub == null || !updatedEndpointsInfo.getLeaderId().equals(leaderStub.leaderId))) {
            log.info("DataNode {} update leaderId; updated Id is: {}", dataNodeId, updatedEndpointsInfo.getLeaderId());

            //get leader address first
            String updatedLeaderAddress = updatedEndpointsInfo.getEndpointsMap().get(updatedEndpointsInfo.getLeaderId());
            //build channel and stub
            ManagedChannel updatedToLeaderChannel = ManagedChannelBuilder.forTarget(updatedLeaderAddress).usePlaintext().build();
            cluster.external.shard.proto.ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub updatedLeaderStub = ShardRequestHandlerGrpc.newFutureStub(updatedToLeaderChannel);
            leaderStub = new LeaderStub(updatedEndpointsInfo.getLeaderId(), updatedLeaderStub);
        }
    }

    @Override
    public ClusterEndpointsInfo getClusterReport() {
        //cluster info(update leader info)
        return this.clusterEndpointsInfoCache.get();
    }

    @Override
    public cluster.external.shard.proto.ShardResponse getShardReport(GetAllShardRequest getAllShardRequest) {
        ShardResponse response = callHelper((ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub)
                -> stub.getAll(getAllShardRequest)).join();
        return response;
    }

    @Override
    public cluster.external.shard.proto.ShardResponse putShardInfo(cluster.external.shard.proto.PutShardRequest request) {
        ShardResponse response = callHelper((ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub)
                -> stub.put(request)).join();
        return response;
    }

    @Override
    public ShardResponse getShardInfo(GetShardRequest request) {
        ShardResponse response = callHelper((ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub)
                -> stub.get(request)).join();
        return response;
    }

    private CompletableFuture<ShardResponse> callHelper(
            Function<ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub, ListenableFuture<ShardResponse>> func) {
        return new Helper(func).implmentCall();
    }

    private class Helper {
        final CompletableFuture<ShardResponse> future = new CompletableFuture<>();
        final Function<ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub, ListenableFuture<ShardResponse>> func;

        Helper(Function<ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub, ListenableFuture<ShardResponse>> func) {
            this.func = func;
        }

        private CompletableFuture<ShardResponse> implmentCall() {
            if (leaderStub == null) {
                executor.schedule(this::implmentCall, 10, MILLISECONDS);
                return future;
            }

            ListenableFuture<ShardResponse> rawFuture = func.apply(leaderStub.stub);
            rawFuture.addListener(() -> handleRpcResult(rawFuture), executor);

            return future;
        }

        void handleRpcResult(ListenableFuture<ShardResponse> rawFuture) {
            try {
                future.complete(rawFuture.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            } catch (ExecutionException e) {
                Throwable t = getRootCause(e);

                if (t instanceof StatusRuntimeException && t.getMessage().contains("RAFT_ERROR")) {
                    StatusRuntimeException ex = (StatusRuntimeException) t;
                    if (ex.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                            || ex.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED) {
                        executor.schedule(this::implmentCall, 10, MILLISECONDS);
                        return;
                    }
                }

                future.completeExceptionally(new RuntimeException(t));
            }
        }
    }

}
