package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.RaftMessageRequest;
import cluster.internal.raft.proto.VoteResponseProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.VoteResponse;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class VoteResponseOrBuilder implements VoteResponse, VoteResponseBuilder, RaftMessageRequestAware {

    private VoteResponseProto.Builder builder;
    private VoteResponseProto response;
    private RaftEndpoint sender;

    public VoteResponseOrBuilder() {
        this.builder = VoteResponseProto.newBuilder();
    }

    public VoteResponseOrBuilder(VoteResponseProto response) {
        this.response = response;
        this.sender = RaftNodeEndpoint.wrap(response.getSender());
    }

    public VoteResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setGranted(boolean granted) {
        builder.setGranted(granted);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setVoteResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "VoteResponse{builder=" + builder + "}";
        }

        return "VoteResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", granted=" + isGranted() + '}';
    }

    @Override
    public boolean isGranted() {
        return response.getGranted();
    }

    @Override
    public Object getGroupId() {
        return response.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return response.getTerm();
    }

}
