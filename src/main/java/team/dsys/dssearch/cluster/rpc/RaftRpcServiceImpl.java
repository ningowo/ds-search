package team.dsys.dssearch.cluster.rpc;

import cluster.proto.RaftCommunicationServiceGrpc;
import cluster.proto.RaftMessageRequest;
import cluster.proto.RaftMessageResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.*;
import static team.dsys.dssearch.cluster.rpc.utils.Serialization.wrap;

@Singleton
public class RaftRpcServiceImpl implements RaftRpcService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcService.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final Map<RaftEndpoint, RaftRpcStub> stubs = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingEndpoints = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final long grpcTimeoutSecs;

    @Inject
    public RaftRpcServiceImpl(@Named(NODE_ENDPOINT_KEY) RaftEndpoint nodeEndpoint,
                              @Named(CONFIG_KEY) ClusterServiceConfig config,
                              @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses) {
        this.localEndpoint = nodeEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.grpcTimeoutSecs = config.getRpcConfig().getGrpcTimeoutSecs();
    }

    @PreDestroy
    public void shutdown() {
        stubs.values().forEach(RaftRpcStub::shutdownSilently);
        stubs.clear();
        executor.shutdownNow();

        LOGGER.info(localEndpoint.getId() + " RaftMessageRpc service is shutting down...");
    }

    //add other enpoint's address to current node's contact info
    @Override
    public void addAddress(@Nonnull RaftEndpoint endpoint, @Nonnull String address) {
        requireNonNull(endpoint);
        requireNonNull(address);

        String currentAddress = addresses.put(endpoint, address);
        if (currentAddress == null) {
            LOGGER.info("{} added address: {} for {}", localEndpoint.getId(), address, endpoint.getId());
        } else if (!currentAddress.equals(address)) {
            LOGGER.warn("{} replaced current address: {} with new address: {} for {}", localEndpoint.getId(),
                    currentAddress, address, endpoint.getId());
        }
    }

    @Override
    public Map<RaftEndpoint, String> getAddresses() {
        return new HashMap<>(addresses);
    }

    /**
     * Send raftmessage from current node to target node
     */
    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        RaftRpcStub stub = getOrCreateStub(requireNonNull(target));
        if (stub != null) {
            executor.submit(() -> {
                stub.send(message);
            });
        }
    }

    /**
     * Check the availability of target node
     * @param endpoint
     * @return
     */
    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return stubs.containsKey(endpoint);
    }

    private RaftRpcStub getOrCreateStub(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send Raft message to itself...", localEndpoint.getId());
            return null;
        }

        RaftRpcStub stub = stubs.get(target);
        if (stub != null) {
            return stub;
        } else if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        return connect(target);
    }

    private RaftRpcStub connect(RaftEndpoint target) {
        if (!initializingEndpoints.add(target)) {
            return null;
        }

        try {
            String address = addresses.get(target);
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry().usePlaintext()
                    // .directExecutor()
                    .build();

            RaftCommunicationServiceGrpc.RaftCommunicationServiceStub replicationStub = RaftCommunicationServiceGrpc.newStub(channel);
            // .withDeadlineAfter(rpcTimeoutSecs, SECONDS);
            RaftRpcStub stub = new RaftRpcStub(target, channel);
            stub.raftMessageSender = replicationStub.handleRaftMessage(new ResponseStreamObserver(stub));

            stubs.put(target, stub);

            return stub;
        } finally {
            initializingEndpoints.remove(target);
        }
    }

    private void checkChannel(RaftRpcStub stub) {
        if (stubs.remove(stub.targetEndpoint, stub)) {
            stub.shutdownSilently();
        }

        delayChannelCreation(stub.targetEndpoint);
    }

    private void delayChannelCreation(RaftEndpoint target) {
        if (initializingEndpoints.add(target)) {
            LOGGER.debug("{} delaying channel creation to {}.", localEndpoint.getId(), target.getId());
            try {
                executor.schedule(() -> {
                    initializingEndpoints.remove(target);
                }, 1, SECONDS);
            } catch (RejectedExecutionException e) {
                LOGGER.warn("{} could not schedule task for channel creation to: {}.", localEndpoint.getId(),
                        target.getId());
                initializingEndpoints.remove(target);
            }
        }
    }

    private class RaftRpcStub {
        final RaftEndpoint targetEndpoint;
        final ManagedChannel channel;
        StreamObserver<RaftMessageRequest> raftMessageSender;

        RaftRpcStub(RaftEndpoint targetEndpoint, ManagedChannel channel) {
            this.targetEndpoint = targetEndpoint;
            this.channel = channel;
        }

        void shutdownSilently() {
            raftMessageSender.onCompleted();
            channel.shutdown();
        }

        public void send(@Nonnull RaftMessage message) {
            try {
                raftMessageSender.onNext(wrap(message));
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error(localEndpoint.getId() + " failure during sending " + message.getClass().getSimpleName()
                            + " to " + targetEndpoint, t);
                } else {
                    LOGGER.error("{} failure during sending {} to {}. Exception: {} Message: {}", localEndpoint.getId(),
                            message.getClass().getSimpleName(), targetEndpoint, t.getClass().getSimpleName(),
                            t.getMessage());
                }
            }
        }
    }

    private class ResponseStreamObserver implements StreamObserver<RaftMessageResponse> {
        final RaftRpcStub stub;

        private ResponseStreamObserver(RaftRpcStub stub) {
            this.stub = stub;
        }

        @Override
        public void onNext(RaftMessageResponse response) {
            LOGGER.warn("{} received {} from Raft RPC stream to {}", localEndpoint.getId(), response,
                    stub.targetEndpoint.getId());
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpoint.getId() + " streaming Raft RPC to " + stub.targetEndpoint.getId()
                        + " has failed.", t);
            } else {
                LOGGER.error("{} Raft RPC stream to {} has failed. Exception: {} Message: {}", localEndpoint.getId(),
                        stub.targetEndpoint.getId(), t.getClass().getSimpleName(), t.getMessage());
            }

            checkChannel(stub);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} Raft RPC stream to {} has completed.", localEndpoint.getId(),
                    stub.targetEndpoint.getId());
        }

    }

}