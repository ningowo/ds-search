package team.dsys.dssearch.cluster.raft;

import cluster.internal.raft.proto.RaftEndpointProto;
import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;



public class RaftNodeEndpoint implements RaftEndpoint {

    private static final ConcurrentMap<String, RaftNodeEndpoint> cache = new ConcurrentHashMap<>();
    private RaftEndpointProto endpoint;

    public RaftNodeEndpoint(RaftEndpointProto endpoint) {
        this.endpoint = endpoint;
    }

    //wrap and unwrap are interconversion between raftendpointproto and raftnodeenpoint
    public static RaftNodeEndpoint wrap(@Nonnull RaftEndpointProto endpoint) {
        return cache.computeIfAbsent(endpoint.getId(), id -> new RaftNodeEndpoint(endpoint));
    }

    public static RaftEndpointProto unwrap(@Nullable RaftEndpoint endpoint) {
        return endpoint != null ? ((RaftNodeEndpoint) endpoint).getEndpoint() : null;
    }

    public RaftEndpointProto getEndpoint() {
        return endpoint;
    }

    @Override
    public int hashCode() {
        return endpoint.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RaftNodeEndpoint that = (RaftNodeEndpoint) o;

        return endpoint.equals(that.endpoint);
    }

    @Override
    public String toString() {
        return "Current NodeEndpoint{" + "id=" + getId() + '}';
    }

    @Nonnull
    @Override
    public Object getId() {
        return endpoint.getId();
    }

}

