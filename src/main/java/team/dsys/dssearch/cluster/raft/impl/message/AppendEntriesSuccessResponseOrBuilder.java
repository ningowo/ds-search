package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.AppendEntriesSuccessResponseProto;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class AppendEntriesSuccessResponseOrBuilder implements AppendEntriesSuccessResponse,
        AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder, RaftMessageRequestAware {

    private AppendEntriesSuccessResponseProto.Builder builder;
    private AppendEntriesSuccessResponseProto response;
    private RaftEndpoint sender;

    public AppendEntriesSuccessResponseOrBuilder() {
        this.builder = AppendEntriesSuccessResponseProto.newBuilder();
    }

    public AppendEntriesSuccessResponseOrBuilder(AppendEntriesSuccessResponseProto response) {
        this.response = response;
        this.sender = RaftNodeEndpoint.wrap(response.getSender());
    }

    public AppendEntriesSuccessResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setLastLogIndex(long lastLogIndex) {
        builder.setLastLogIndex(lastLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setAppendEntriesSuccessResponse(response);
    }

    @Override
    public long getLastLogIndex() {
        return response.getLastLogIndex();
    }

    @Override
    public long getQuerySequenceNumber() {
        return response.getQuerySequenceNumber();
    }

    @Override
    public long getFlowControlSequenceNumber() {
        return response.getFlowControlSequenceNumber();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "AppendEntriesSuccessResponse{builder=" + builder + "}";
        }

        return "AppendEntriesSuccessResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", "
                + "term=" + getTerm() + ", lastLogIndex=" + getLastLogIndex() + ", querySequenceNumber="
                + getQuerySequenceNumber() + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + '}';
    }

}
