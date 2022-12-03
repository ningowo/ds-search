package team.dsys.dssearch.cluster.module;

import cluster.proto.ManagementRequestHandlerGrpc;
import cluster.proto.RaftCommunicationServiceGrpc;
import cluster.proto.ShardRequestHandlerGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.RaftModelFactory;
import io.microraft.statemachine.StateMachine;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.raft.LocalStateMachine;
import team.dsys.dssearch.cluster.raft.listener.ClusterEndpointsListener;
import team.dsys.dssearch.cluster.raft.model.RaftModelFactoryImpl;
import team.dsys.dssearch.cluster.raft.report.RaftNodeReportSupplier;
import team.dsys.dssearch.cluster.raft.supplier.RaftNodeSupplier;
import team.dsys.dssearch.cluster.rpc.*;
import team.dsys.dssearch.cluster.rpc.impl.ClusterHealthManagementHandler;
import team.dsys.dssearch.cluster.rpc.impl.ShardRequestHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;

/**
 * Setting module info for the injector
 */
public class ClusterServiceModule extends AbstractModule {

    public static final String CONFIG_KEY = "Config";
    public static final String NODE_ENDPOINT_KEY = "NodeEndpoint";
    public static final String INITIAL_ENDPOINTS_KEY = "InitialEndpoints";
    public static final String RAFT_ENDPOINT_ADDRESSES_KEY = "RaftEndpointAddresses";
    public static final String RAFT_NODE_SUPPLIER_KEY = "RaftNodeSupplier";

    private final ClusterServiceConfig config;
    private final RaftEndpoint nodeEndpoint;
    private final List<RaftEndpoint> initialEndpoints;
    private final Map<RaftEndpoint, String> addresses;

    public ClusterServiceModule(ClusterServiceConfig config, RaftEndpoint nodeEndpoint, List<RaftEndpoint> initialEndpoints,
                          Map<RaftEndpoint, String> addresses) {
        this.config = config;
        this.nodeEndpoint = nodeEndpoint;
        this.initialEndpoints = initialEndpoints;
        this.addresses = addresses;
    }

    @Override
    protected void configure() {
        bind(ClusterServiceConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(RaftEndpoint.class).annotatedWith(named(NODE_ENDPOINT_KEY)).toInstance(nodeEndpoint);
        bind(new TypeLiteral<Collection<RaftEndpoint>>() {
        }).annotatedWith(named(INITIAL_ENDPOINTS_KEY)).toInstance(initialEndpoints);
        bind(new TypeLiteral<Map<RaftEndpoint, String>>() {
        }).annotatedWith(named(RAFT_ENDPOINT_ADDRESSES_KEY)).toInstance(addresses);

        //clusterEndpointsListener: update and expose cluster information to client(internal call)
        bind(RaftNodeReportSupplier.class).to(ClusterEndpointsListener.class);
        bind(StateMachine.class).to(LocalStateMachine.class);
        bind(RaftModelFactory.class).to(RaftModelFactoryImpl.class);
        bind(RaftCommunicationServiceGrpc.RaftCommunicationServiceImplBase.class).to(RaftMessageHandler.class);
        bind(GrpcServer.class).to(GrpcServerImpl.class);
        bind(RaftRpcService.class).to(RaftRpcServiceImpl.class);
        bind(ShardRequestHandlerGrpc.ShardRequestHandlerImplBase.class).to(ShardRequestHandler.class);
        bind(ManagementRequestHandlerGrpc.ManagementRequestHandlerImplBase.class).to(ClusterHealthManagementHandler.class);

        bind(new TypeLiteral<Supplier<RaftNode>>() {
        }).annotatedWith(named(RAFT_NODE_SUPPLIER_KEY)).to(RaftNodeSupplier.class);
    }

}

