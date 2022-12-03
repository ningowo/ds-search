package team.dsys.dssearch.cluster.rpc;

import cluster.proto.ClusterListenServiceGrpc;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.microraft.RaftEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.raft.report.RaftNodeReportSupplier;
import team.dsys.dssearch.cluster.rpc.impl.ClusterHealthManagementHandler;
import team.dsys.dssearch.cluster.rpc.impl.ShardRequestHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

import static team.dsys.dssearch.cluster.module.ClusterServiceModule.CONFIG_KEY;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.NODE_ENDPOINT_KEY;

@Singleton
public class GrpcServerImpl implements GrpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);

    private final RaftEndpoint nodeEndpoint;
    private final Server server;

    @Inject
    public GrpcServerImpl(@Named(NODE_ENDPOINT_KEY) RaftEndpoint nodeEndpoint,
                         @Named(CONFIG_KEY) ClusterServiceConfig config, ShardRequestHandler shardRequestHandler,
                         RaftMessageHandler raftMessageHandler, ClusterHealthManagementHandler clusterHealthManagementHandler,
                         RaftNodeReportSupplier raftNodeReportSupplier) {
        this.nodeEndpoint = nodeEndpoint;
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Class<? extends ServerChannel> channelType = NioServerSocketChannel.class;
        LOGGER.info("SiyingChen " + config.getNodeEndpointConfig().getSocketAddress());
        this.server = NettyServerBuilder.forAddress(config.getNodeEndpointConfig().getSocketAddress())
                .bossEventLoopGroup(boss).workerEventLoopGroup(worker).channelType(channelType)
                .addService(shardRequestHandler).addService(raftMessageHandler).addService(clusterHealthManagementHandler)
                .addService((ClusterListenServiceGrpc.ClusterListenServiceImplBase) raftNodeReportSupplier).directExecutor().build();
    }

    @PostConstruct
    public void start() {
        try {
            //automatically start grpc server when constructed
            server.start();
            LOGGER.info(nodeEndpoint.getId() + "SiyingChen RpcServer started.");
        } catch (IOException e) {
            throw new ClusterServerException(nodeEndpoint.getId() + " RpcServer start failed!", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOGGER.info(nodeEndpoint.getId() + " shutting down RpcServer...");

        try {
            server.shutdownNow();
            LOGGER.info(nodeEndpoint.getId() + " RpcServer is shut down.");
        } catch (Throwable t) {
            String message = nodeEndpoint.getId() + " failure during termination of RpcServer";
            LOGGER.error(message, t);
        }
    }

    @Override
    public void awaitTermination() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn(nodeEndpoint.getId() + " await termination of RpcServer interrupted!");
        }
    }

}