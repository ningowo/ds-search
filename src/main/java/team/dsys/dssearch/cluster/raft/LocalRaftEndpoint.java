package team.dsys.dssearch.cluster.raft;

import io.microraft.RaftEndpoint;

public class LocalRaftEndpoint implements RaftEndpoint {
    private String endpoint;

    public LocalRaftEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    //todo: equals
    //todo: hashcode
    //todo: toString

    @Override
    public Object getId() {
        return endpoint;
    }
}
