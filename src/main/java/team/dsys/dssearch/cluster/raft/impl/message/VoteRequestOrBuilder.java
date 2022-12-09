package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.RaftMessageRequest;
import cluster.internal.raft.proto.VoteRequestProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteRequest.VoteRequestBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class VoteRequestOrBuilder implements VoteRequest, VoteRequestBuilder, RaftMessageRequestAware {

    private VoteRequestProto.Builder builder;
    private VoteRequestProto request;
    private RaftEndpoint sender;

    public VoteRequestOrBuilder() {
        this.builder = VoteRequestProto.newBuilder();
    }

    public VoteRequestOrBuilder(VoteRequestProto request) {
        this.request = request;
        this.sender = RaftNodeEndpoint.wrap(request.getSender());
    }

    public VoteRequestProto getRequest() {
        return request;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setLastLogTerm(int lastLogTerm) {
        builder.setLastLogTerm(lastLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setLastLogIndex(long lastLogIndex) {
        builder.setLastLogIndex(lastLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public VoteRequestBuilder setSticky(boolean sticky) {
        builder.setSticky(sticky);
        return this;
    }

    @Nonnull
    @Override
    public VoteRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setVoteRequest(request);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "VoteRequest{builder=" + builder + "}";
        }

        return "VoteRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", lastLogTerm=" + getLastLogTerm() + ", lastLogIndex=" + getLastLogIndex() + ", sticky=" + isSticky()
                + '}';
    }

    @Override
    public int getLastLogTerm() {
        return request.getLastLogTerm();
    }

    @Override
    public long getLastLogIndex() {
        return request.getLastLogIndex();
    }

    @Override
    public boolean isSticky() {
        return request.getSticky();
    }

    @Override
    public Object getGroupId() {
        return request.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return request.getTerm();
    }

}
