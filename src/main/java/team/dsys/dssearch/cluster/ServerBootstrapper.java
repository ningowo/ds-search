package team.dsys.dssearch.cluster;

import cluster.proto.RaftEndpointProto;
import io.microraft.RaftEndpoint;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.config.NodeEndpointConfig;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ServerBootstrapper implements Supplier<ClusterServiceImpl> {
    final ClusterServiceConfig config;

    public ServerBootstrapper(ClusterServiceConfig config) {
        this.config = config;
    }

    @Override
    public ClusterServiceImpl get() {
        RaftEndpointProto localEndpoint = toProtoRaftEndpoint(config.getNodeEndpointConfig());
        List<RaftEndpoint> initialEndpoints = getInitialEndpoints(config);
        Map<RaftEndpoint, String> endpointAddresses = getEndpointAddresses(config);
        return new ClusterServiceImpl(config, RaftNodeEndpoint.wrap(localEndpoint), initialEndpoints, endpointAddresses);
    }

    private static RaftEndpointProto toProtoRaftEndpoint(NodeEndpointConfig endpointConfig) {
        return RaftEndpointProto.newBuilder().setId(endpointConfig.getId()).build();
    }

    private List<RaftEndpoint> getInitialEndpoints(ClusterServiceConfig config) {
        List<RaftEndpoint> initialEndpoints = config.getClusterConfig().getInitialEndpoints().stream()
                .map(ServerBootstrapper::toProtoRaftEndpoint).map(RaftNodeEndpoint::wrap).collect(toList());
        if (initialEndpoints.size() < 2) {
            throw new ClusterServerException(
                    "Cannot bootstrap new cluster with " + initialEndpoints.size() + " endpoint!");
        }

        return initialEndpoints;
    }

    private Map<RaftEndpoint, String> getEndpointAddresses(ClusterServiceConfig config) {
        return config.getClusterConfig().getInitialEndpoints().stream().collect(
                toMap(c -> RaftNodeEndpoint.wrap(toProtoRaftEndpoint(c)), NodeEndpointConfig::getAddress));
    }

}