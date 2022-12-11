package team.dsys.dssearch.model;

import cluster.external.shard.proto.DataNodeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class Shards {

    public int shardId;

    public DataNodeInfo primary;

    public List<DataNodeInfo> replicaList;

}
