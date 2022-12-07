package team.dsys.dssearch.shard;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;

@Getter
@Setter
public class Shards {

    public HashMap<Integer, Shard> idToShards;

}
