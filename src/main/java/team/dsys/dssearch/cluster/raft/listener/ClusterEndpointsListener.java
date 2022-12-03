package team.dsys.dssearch.cluster.raft.listener;

import cluster.proto.ClusterEndpointsInfo;
import cluster.proto.ClusterEndpointsRequest;
import cluster.proto.ClusterEndpointsResponse;
import cluster.proto.ClusterListenServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.raft.report.RaftNodeReportSupplier;
import team.dsys.dssearch.cluster.rpc.RaftRpcService;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.PERIODIC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.CONFIG_KEY;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.NODE_ENDPOINT_KEY;

/**
 * Implement clusterListenerService and RaftNodeReportSupplier
 */
@Singleton
public class ClusterEndpointsListener extends ClusterListenServiceGrpc.ClusterListenServiceImplBase
        implements RaftNodeReportSupplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterEndpointsListener.class);
    private static final long CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS = SECONDS.toMillis(30);

    private final Map<String, StreamObserver<ClusterEndpointsResponse>> observers = new ConcurrentHashMap<>();
    private final ClusterServiceConfig config;
    private final RaftEndpoint nodeEndpoint;
    private final RaftRpcService raftRpcService;
    private volatile RaftNodeReport lastReport;
    private long raftNodeReportIdlePublishTimestamp;

    @Inject
    public ClusterEndpointsListener(@Named(CONFIG_KEY) ClusterServiceConfig config,
                                             @Named(NODE_ENDPOINT_KEY) RaftEndpoint nodeEndpoint, RaftRpcService raftRpcService) {
        this.config = config;
        this.nodeEndpoint = nodeEndpoint;
        this.raftRpcService = raftRpcService;
        this.raftNodeReportIdlePublishTimestamp = System.currentTimeMillis() - CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS;
    }

//for client to connect to the cluster, get cluster nodeEndpoint info
    @Override
    public void listenClusterEndpoints(ClusterEndpointsRequest request,
                                       StreamObserver<ClusterEndpointsResponse> responseObserver) {
        StreamObserver<ClusterEndpointsResponse> prev = observers.put(request.getClientId(), responseObserver);
        if (prev != null) {
            LOGGER.warn("{} completing already existing stream observer for {}.", nodeEndpoint.getId(),
                    request.getClientId());
            prev.onCompleted();
        }

        LOGGER.debug("{} registering client: {}.", nodeEndpoint.getId(), request.getClientId());

        if (lastReport != null) {
            try {
                responseObserver.onNext(createResponse(lastReport));
                LOGGER.debug("{} sent {} to {}.", nodeEndpoint.getId(), lastReport, request.getClientId());
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(nodeEndpoint.getId() + " could not send cluster endpoints to " + request.getClientId(),
                            t);
                } else {
                    LOGGER.warn("{} could not send cluster endpoints to {}. Exception: {} Message: {}",
                            nodeEndpoint.getId(), request.getClientId(), t.getClass().getSimpleName(), t.getMessage());
                }

                observers.remove(request.getClientId(), responseObserver);
            }
        }
    }

    private ClusterEndpointsResponse createResponse(RaftNodeReport report) {
        RaftGroupMembers committedMembers = report.getCommittedMembers();

        ClusterEndpointsInfo.Builder endpointsInfoBuilder = ClusterEndpointsInfo.newBuilder();
        endpointsInfoBuilder.setClusterId(config.getClusterConfig().getId());
        endpointsInfoBuilder.setEndpointsCommitIndex(committedMembers.getLogIndex());
        if (report.getTerm().getLeaderEndpoint() != null) {
            endpointsInfoBuilder.setLeaderId((String) report.getTerm().getLeaderEndpoint().getId());
        }

        endpointsInfoBuilder.setTerm(report.getTerm().getTerm());

        raftRpcService.getAddresses().entrySet().stream()
                .filter(e -> committedMembers.getMembers().contains(e.getKey()))
                .forEach(e -> endpointsInfoBuilder.putEndpoints((String) e.getKey().getId(), e.getValue()));

        return ClusterEndpointsResponse.newBuilder().setEndpointsInfo(endpointsInfoBuilder.build()).build();
    }

    @Override
    public void accept(@Nonnull RaftNodeReport report) {
        long now = System.currentTimeMillis();
        boolean publishForIdleState = now - raftNodeReportIdlePublishTimestamp >= CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS;
        if (publishForIdleState) {
            raftNodeReportIdlePublishTimestamp = now;
        }

        if (publishForIdleState || report.getReason() != PERIODIC) {
            publish(report);
        }
    }

    private void publish(RaftNodeReport report) {
        ClusterEndpointsResponse response = createResponse(report);
        lastReport = report;
        Iterator<Entry<String, StreamObserver<ClusterEndpointsResponse>>> it = observers.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StreamObserver<ClusterEndpointsResponse>> e = it.next();
            String clientId = e.getKey();
            StreamObserver<ClusterEndpointsResponse> observer = e.getValue();
            try {
                LOGGER.debug("{} sending {} to client: {}.", nodeEndpoint.getId(), report, clientId);
                observer.onNext(response);
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(nodeEndpoint.getId() + " could not send cluster endpoints to " + clientId, t);
                } else {
                    LOGGER.warn("{} could not send cluster endpoints to {}. Exception: {} Message: {}",
                            nodeEndpoint.getId(), clientId, t.getClass().getSimpleName(), t.getMessage());
                }
                it.remove();

                observer.onCompleted();
            }
        }
    }



    @Override
    public RaftNodeReport get() {
        return lastReport;
    }

    @Override
    public Consumer<RaftNodeReport> andThen(Consumer<? super RaftNodeReport> after) {
        return RaftNodeReportSupplier.super.andThen(after);
    }
}

