package team.dsys.dssearch.cluster.shard;

public class Shards {

    Shard primary;

    Shard replica;

    class Shard {

        int shardId;

        ShardRouting routing;
    }

}
