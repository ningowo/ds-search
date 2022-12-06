package team.dsys.dssearch.cluster;

import team.dsys.dssearch.shard.*;

/**
 * cluster nodes communicates using RPC
 */
public interface ClusterService {

    // todo
    Integer getNodeByShardId(long shardId);

    // todo
    Shards getShardOnCurrentNode();


}
