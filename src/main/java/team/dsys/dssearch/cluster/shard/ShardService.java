package team.dsys.dssearch.cluster.shard;

import java.util.HashMap;
import java.util.List;

public class ShardService {

    // categories doc by ids into nodes they stored
    // key - node id, val - docs on that node
    public HashMap<Integer, List<Integer>> shard(List<Integer> docIds) {

        return null;
    }

    public boolean sendTranslog(TransLog log, Shards shards) {
        return false;
    }

    // NodeEnvironment

}
