package team.dsys.dssearch.cluster;

import team.dsys.dssearch.cluster.routing.*;
import team.dsys.dssearch.cluster.shard.*;

// org.elasticsearch.cluster.ClusterState;

public class ClusterState {

    String clusterName;

    // node routing
    RoutingTable routingTable;

    // shard routing
    // es: ClusterState - routingNodes -
    // Map<String, RoutingNode> nodesToShards - (node, LinkedHashMap<ShardId, ShardRouting> shards)

    Shards shards;

}
