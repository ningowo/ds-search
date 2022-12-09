package team.dsys.dssearch.cluster;

/**
 * cluster nodes communicates using RPC
 */
public interface ClusterService {

    // todo
    Integer getNodeByShardId(long shardId);

    // todo
//    Shards getShardOnCurrentNode();


}
