package team.dsys.dssearch.cluster;

import team.dsys.dssearch.shard.ShardRouting;
import team.dsys.dssearch.shard.ShardRoutingTable;
import team.dsys.dssearch.shard.Shards;

public class Node {

    public String addr;

    public int port;

    ShardRoutingTable routingTable;

}