package team.dsys.dssearch.cluster.shard;

import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.HashMap;

public class ShardRoutingTable {

    // key - node id, val - node id
    HashMap<Integer, Integer> shardRoutings;

}
