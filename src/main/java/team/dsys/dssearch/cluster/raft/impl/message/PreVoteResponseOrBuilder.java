package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.PreVoteResponseProto;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class PreVoteResponseOrBuilder implements PreVoteResponse, PreVoteResponseBuilder, RaftMessageRequestAware {

    private PreVoteResponseProto.Builder builder;
    private PreVoteResponseProto response;
    private RaftEndpoint sender;

    public PreVoteResponseOrBuilder() {
        this.builder = PreVoteResponseProto.newBuilder();
    }

    public PreVoteResponseOrBuilder(PreVoteResponseProto response) {
        this.response = response;
        this.sender = RaftNodeEndpoint.wrap(response.getSender());
    }

    public PreVoteResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGranted(boolean granted) {
        builder.setGranted(granted);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setPreVoteResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "PreVoteResponse{builder=" + builder + "}";
        }

        return "PreVoteResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
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
