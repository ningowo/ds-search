package team.dsys.dssearch.cluster;

import team.dsys.dssearch.routing.*;
import team.dsys.dssearch.shard.*;

/**
 * 每个节点需要知道，怎么找到其他节点，和怎么找到其他分片(活着的)
 */

public class ClusterState {

    String clusterName;

    // node routing
    RoutingTable routingTable;

    // shard routing
    // es: ClusterState - routingNodes -
    // Map<String, RoutingNode> nodesToShards - (node, LinkedHashMap<ShardId, ShardRouting> shards)
    Shards shards;

}