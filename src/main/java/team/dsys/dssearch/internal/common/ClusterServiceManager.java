package team.dsys.dssearch.internal.common;


import cluster.external.listener.proto.ClusterEndpointsInfo;
import cluster.external.shard.proto.ShardResponse;
import cluster.external.shard.proto.GetAllShardRequest;
import cluster.external.shard.proto.PutShardRequest;
import cluster.external.shard.proto.GetShardRequest;

public interface ClusterServiceManager {

    ClusterEndpointsInfo getClusterReport();
    ShardResponse getShardReport(GetAllShardRequest getAllShardRequest);
    ShardResponse putShardInfo(PutShardRequest request);
    ShardResponse getShardInfo(GetShardRequest request);



}
