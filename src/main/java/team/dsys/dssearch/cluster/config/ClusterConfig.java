package team.dsys.dssearch.cluster.config;

import java.util.Collections;
import java.util.List;

public class ClusterConfig {
    private String id;
    private List<NodeEndpointConfig> initialNodeEndpoints = Collections.emptyList();
    private String isJoinTo;
}
