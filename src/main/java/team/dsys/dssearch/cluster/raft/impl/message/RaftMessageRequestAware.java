package team.dsys.dssearch.cluster.raft.impl.message;
import cluster.internal.raft.proto.RaftMessageRequest;

public interface RaftMessageRequestAware {

    void populate(RaftMessageRequest.Builder builder);

}
