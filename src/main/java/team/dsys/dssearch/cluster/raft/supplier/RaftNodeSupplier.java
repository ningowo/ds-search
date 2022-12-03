package team.dsys.dssearch.cluster.raft.supplier;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.RaftModelFactory;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.raft.report.RaftNodeReportSupplier;
import team.dsys.dssearch.cluster.rpc.RaftRpcService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static team.dsys.dssearch.cluster.module.ClusterServiceModule.*;

/**
 * Creates and contains a RaftNode instance in the cluster
 */
@Singleton
public class RaftNodeSupplier implements Supplier<RaftNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeSupplier.class);

    private final RaftNode raftNode;

    @Inject
    public RaftNodeSupplier(@Named(CONFIG_KEY) ClusterServiceConfig config,
                            @Named(NODE_ENDPOINT_KEY) RaftEndpoint localEndpoint,
                            @Named(INITIAL_ENDPOINTS_KEY) Collection<RaftEndpoint> initialGroupMembers, RaftRpcService rpcService,
                            StateMachine stateMachine, RaftModelFactory modelFactory, RaftNodeReportSupplier raftNodeReportSupplier) {
        this.raftNode = RaftNode.newBuilder().setGroupId(config.getClusterConfig().getId())
                .setLocalEndpoint(localEndpoint).setInitialGroupMembers(initialGroupMembers)
                .setConfig(config.getRaftConfig()).setTransport(rpcService).setStateMachine(stateMachine)
                .setModelFactory(modelFactory).setRaftNodeReportListener(raftNodeReportSupplier).build();
    }

    @PostConstruct
    public void start() {
        LOGGER.info("{} starting Raft node...", raftNode.getLocalEndpoint().getId());
        try {
            raftNode.start().join();
        } catch (Throwable t) {
            throw new ClusterServerException(raftNode.getLocalEndpoint().getId() + " could not start Raft node!", t);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOGGER.info(raftNode.getLocalEndpoint().getId() + " terminating Raft node...");

        try {
            raftNode.terminate().get(10, TimeUnit.SECONDS);
            LOGGER.info(raftNode.getLocalEndpoint().getId() + " RaftNode is terminated.");
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            String message = raftNode.getLocalEndpoint().getId() + " failure during termination of Raft node";
            LOGGER.info(message, t);
        }
    }

    @Override
    public RaftNode get() {
        return raftNode;
    }

}

