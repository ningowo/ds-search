package team.dsys.dssearch.model;

public class Shard {

    int shardId;

    boolean isPrimary;

    ShardRouting routing;

    public int getShardId() {
        return shardId;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public ShardRouting getRouting() {
        return routing;
    }

}
