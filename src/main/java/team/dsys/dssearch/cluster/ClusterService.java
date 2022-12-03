package team.dsys.dssearch.cluster;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftNodeReport;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;

import javax.annotation.Nonnull;

public interface ClusterService {

    // todo: marker, defined in grpc as a rpc method
    //Integer getNodeByShardId(long shardId);

    /**
     * Get local ClusterServiceConfig (encompassing local node endpoint, initial endpoint and raft setting info)
     * @return ClusterServiceConfig
     */
    @Nonnull
    ClusterServiceConfig getConfig();

    /**
     * Get current node's (join or newly start) id in a cluster(e.g. node2)
     * @return RaftEndpoint
     */
    @Nonnull
    RaftEndpoint getNodeEndpoint();

    /**
     * Get current cluster's info, including cluster's members, their status, terms(election, snapshot, etc.)
     * @return
     */
    @Nonnull
    RaftNodeReport getRaftNodeReport();

    /**
     * Get raft node inside the server
     * @return
     */
    @Nonnull
    RaftNode getRaftNode();

    void shutdown();

    boolean isShutdown();

    void awaitTermination();

}


