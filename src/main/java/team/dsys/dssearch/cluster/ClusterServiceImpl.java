package team.dsys.dssearch.cluster;

import org.springframework.stereotype.Service;
import team.dsys.dssearch.shard.*;

@Service
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
