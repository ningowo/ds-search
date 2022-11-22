package team.dsys.dssearch.cluster.config;

import com.typesafe.config.Config;
import io.microraft.RaftConfig;
import team.dsys.dssearch.config.ClusterConfig;

public class ClusterServerConfig {
    private Config config;
    private NodeEndpointConfig nodeEndpointConfig;
    private ClusterConfig clusterConfig;
    private RaftConfig raftConfig;
    private GrpcConfig grpcConfig;
}
