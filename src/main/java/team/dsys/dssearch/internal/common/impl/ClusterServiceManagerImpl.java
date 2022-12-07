package team.dsys.dssearch.internal.common.impl;

import cluster.external.listener.proto.ClusterEndpointsInfo;
import cluster.external.listener.proto.ClusterEndpointsRequest;
import cluster.external.listener.proto.ClusterEndpointsResponse;
import cluster.external.listener.proto.ClusterListenServiceGrpc;
import cluster.external.shard.proto.*;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.internal.common.ClusterServiceManager;
import team.dsys.dssearch.internal.common.config.ClusterServerCommonConfig;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.getRootCause;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


public class ClusterServiceManagerImpl implements ClusterServiceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterServiceManagerImpl.class);

    private final Integer dataNodeId;
    private final ClusterServerCommonConfig config;
    private Map<String, String> clusterServerAddress = new LinkedHashMap<>();
    private final AtomicReference<ClusterEndpointsInfo> clusterEndpointsInfoCache = new AtomicReference<>(); //update the value atomically(e.g., thread-safe)
    private volatile LeaderStub leaderStub;
    private static final long CREATE_STUB_TIME_LIMIT = SECONDS.toMillis(60);
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

//configFilePath: config automatically read annotation(spring boot)
    public ClusterServiceManagerImpl(Integer dataNodeId, String configFilePath) throws TimeoutException {
        this.dataNodeId = dataNodeId;
        this.config = ClusterServerCommonConfig.getClusterCommonConfig(configFilePath);
        this.clusterServerAddress = config.getClusterServerAddress();
        createClusterServerStub();
    }

    private class LeaderStub {
        private String leaderId;
        private ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub;

        LeaderStub(String leaderId, ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub) {
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
        LOGGER.info("DataNode {} created the cluster server stub for address: {} ", dataNodeId, randomAddress);

        ClusterEndpointsRequest request = ClusterEndpointsRequest.newBuilder().setClientId(dataNodeId.toString()).build();
        long startTime = System.currentTimeMillis();

        try {
            stub.listenClusterEndpoints(request, new ClusterEndpointsResponseObserver(randomAddress));

            while (clusterEndpointsInfoCache.get() == null) {
                if (System.currentTimeMillis() - startTime > CREATE_STUB_TIME_LIMIT) {
                    LOGGER.error("Failed to connect to server {} because of TimeOut Exception", randomAddress);
                    throw new TimeoutException("Connecting to server timeout..");
                }

            }
        } finally {
            try {
                randomConnectedServerChannel.shutdown().awaitTermination(1, SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted exception during gRPC channel close", e);
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
                LOGGER.info("DataNode {} received response from cluster server: {}", dataNodeId, address);
                tryUpdateClusterEndpointsInfo(endpointsInfo);
            } catch(Exception e) {
                LOGGER.error("Response from address: {} failed", address);
            }
        }

        @Override
        public void onError(Throwable t) {
            System.out.println(t.getMessage());
            LOGGER.error("For DataNode {}, Cluster Observer of {} failed. Message: ", dataNodeId, address, t.getMessage());

        }

        @Override
        public void onCompleted() {
            LOGGER.info("For DataNode {}, Cluster Observer of {} completed ", dataNodeId, address);
        }


    }

    private void tryUpdateClusterEndpointsInfo(ClusterEndpointsInfo updatedEndpointsInfo) {
        ClusterEndpointsInfo currentEndpointsInfo = clusterEndpointsInfoCache.get();
        if (currentEndpointsInfo == null || currentEndpointsInfo.getTerm() < updatedEndpointsInfo.getTerm()
        || currentEndpointsInfo.getEndpointsCount() < updatedEndpointsInfo.getEndpointsCount()
        || currentEndpointsInfo.getEndpointsCommitIndex() < updatedEndpointsInfo.getEndpointsCommitIndex()
        || !currentEndpointsInfo.getLeaderId().equals(updatedEndpointsInfo.getLeaderId())) {

            LOGGER.info("DataNode {} updated cluster endpoints info... ", dataNodeId);
            clusterEndpointsInfoCache.set(updatedEndpointsInfo);
            tryUpdateLeaderInfo(updatedEndpointsInfo);
        }

        return;
    }



    private void tryUpdateLeaderInfo(ClusterEndpointsInfo updatedEndpointsInfo) {
        if (!isNullOrEmpty(updatedEndpointsInfo.getLeaderId()) && (leaderStub == null || !updatedEndpointsInfo.getLeaderId().equals(leaderStub.leaderId))) {
            LOGGER.info("DataNode {} update leaderId; updated Id is: {}", dataNodeId, updatedEndpointsInfo.getLeaderId());

            //get leader address first
            String updatedLeaderAddress = updatedEndpointsInfo.getEndpointsMap().get(updatedEndpointsInfo.getLeaderId());
            //build channel and stub
            ManagedChannel updatedToLeaderChannel = ManagedChannelBuilder.forTarget(updatedLeaderAddress).usePlaintext().build();
            ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub updatedLeaderStub = ShardRequestHandlerGrpc.newFutureStub(updatedToLeaderChannel);
            leaderStub = new LeaderStub(updatedEndpointsInfo.getLeaderId(), updatedLeaderStub);

        }
    }

    @Override
    public ClusterEndpointsInfo getClusterReport() {
        //cluster info(update leader info)
        return this.clusterEndpointsInfoCache.get();
    }

    @Override
    public ShardResponse getShardReport(GetAllShardRequest getAllShardRequest) {
        ShardResponse response = callHelper((ShardRequestHandlerGrpc.ShardRequestHandlerFutureStub stub)
                -> stub.getAll(getAllShardRequest)).join();
        return response;
    }

    @Override
    public ShardResponse putShardInfo(PutShardRequest request) {
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





    public static void main(String[] args) throws TimeoutException {
        String p = "/Users/chensiying/cs61b/search-engine-project/src/main/java/team/dsys/dssearch/internal/common/config/cluster.conf";
        ClusterServiceManagerImpl manager = new ClusterServiceManagerImpl(1, p);
        System.out.println(manager.getClusterReport());


        List<ShardInfo> shardInfoList = new ArrayList<>();
        shardInfoList.add(ShardInfo.newBuilder().setShardId(1).setIsPrimary(true).build());
        shardInfoList.add(ShardInfo.newBuilder().setShardId(2).setIsPrimary(false).build());
        shardInfoList.add(ShardInfo.newBuilder().setShardId(4).setIsPrimary(true).build());
        shardInfoList.add(ShardInfo.newBuilder().setShardId(1).setIsPrimary(false).build());
        PutShardRequest req = PutShardRequest.newBuilder().
                          setDataNodeInfo(DataNodeInfo.newBuilder().setDataNodeId(1).setAddress("localhost:4001").build()).addAllShardInfo(shardInfoList).build();
        System.out.println(manager.putShardInfo(req));

        List<ShardInfo> shardInfoList2 = new ArrayList<>();
        shardInfoList2.add(ShardInfo.newBuilder().setShardId(5).setIsPrimary(false).build());
        shardInfoList2.add(ShardInfo.newBuilder().setShardId(3).setIsPrimary(false).build());
        shardInfoList2.add(ShardInfo.newBuilder().setShardId(4).setIsPrimary(true).build());
        shardInfoList2.add(ShardInfo.newBuilder().setShardId(2).setIsPrimary(false).build());
        PutShardRequest req2 = PutShardRequest.newBuilder().
                setDataNodeInfo(DataNodeInfo.newBuilder().setDataNodeId(2).setAddress("localhost:4002").build()).addAllShardInfo(shardInfoList2).build();
        System.out.println(manager.putShardInfo(req2));

        List<ShardInfo> shardInfoList3 = new ArrayList<>();
        shardInfoList3.add(ShardInfo.newBuilder().setShardId(1).setIsPrimary(true).build());
        shardInfoList3.add(ShardInfo.newBuilder().setShardId(2).setIsPrimary(false).build());
        shardInfoList3.add(ShardInfo.newBuilder().setShardId(3).setIsPrimary(true).build());
        shardInfoList3.add(ShardInfo.newBuilder().setShardId(5).setIsPrimary(true).build());
        PutShardRequest req3 = PutShardRequest.newBuilder().
                setDataNodeInfo(DataNodeInfo.newBuilder().setDataNodeId(3).setAddress("localhost:4003").build()).addAllShardInfo(shardInfoList3).build();
        System.out.println(manager.putShardInfo(req3));

//        List<Integer> shardIdList = new ArrayList<>();
//        shardIdList.add(5);
//        shardIdList.add(3);
//        GetShardRequest req1 = GetShardRequest.newBuilder().addAllShardId(shardIdList).setMinCommitIndex(-1L).build();
//        ShardResponse response = manager.getShardInfo(req1);
//        System.out.println(response);

//        for (int i = 1; i <= 5; i++) {
//            int finalI = i;
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    List<Integer> shardIdListThreadTest = new ArrayList<>();
//                    shardIdListThreadTest.add(finalI);
//                    GetShardRequest req1 = GetShardRequest.newBuilder().addAllShardId(shardIdListThreadTest).setMinCommitIndex(-1L).build();
//                    ShardResponse response1 = manager.getShardInfo(req1);
//                    System.out.println(response1);
//                }
//            }).start();
//        }

        System.out.println(manager.getShardReport(GetAllShardRequest.newBuilder().build()));





    }
}
