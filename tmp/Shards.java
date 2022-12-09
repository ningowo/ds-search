package team.dsys.dssearch.model;

import lombok.Getter;
import lombok.Setter;
import team.dsys.dssearch.model.Shard;

import java.util.HashMap;

@Getter
@Setter
public class Shards {

    public HashMap<Integer, Shard> idToShards;

}
