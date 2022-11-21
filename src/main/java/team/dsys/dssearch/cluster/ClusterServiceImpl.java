package team.dsys.dssearch.cluster;

import team.dsys.dssearch.shard.*;

public class ClusterServiceImpl implements ClusterService {

    private static Shards shardsOnCurrentNode;

    @Override
    public Integer getNodeByShardId(long shardId) {
        return null;
    }

    @Override
    public Shards getShardOnCurrentNode() {
        return null;
    }

    public static Shards getShardsOnCurrentNode() {
        return shardsOnCurrentNode;
    }
}
