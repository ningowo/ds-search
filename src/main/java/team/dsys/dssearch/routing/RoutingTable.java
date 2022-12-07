package team.dsys.dssearch.routing;

import team.dsys.dssearch.cluster.Node;

import java.util.HashMap;

/**
 * 节点的路由
 */

public class RoutingTable {

    public HashMap<NodeRouting, Node> getNodeAddrs() {
        return nodeAddrs;
    }

    HashMap<NodeRouting, team.dsys.dssearch.cluster.Node> nodeAddrs;

}
