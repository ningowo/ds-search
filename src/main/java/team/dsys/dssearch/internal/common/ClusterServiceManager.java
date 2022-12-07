package team.dsys.dssearch.internal.common;

import cluster.external.listener.proto.ClusterEndpointsInfo;
import cluster.external.shard.proto.*;

public interface ClusterServiceManager {

    ClusterEndpointsInfo getClusterReport();
    ShardResponse getShardReport(GetAllShardRequest getAllShardRequest);
    ShardResponse putShardInfo(PutShardRequest request);
    ShardResponse getShardInfo(GetShardRequest request);



}
