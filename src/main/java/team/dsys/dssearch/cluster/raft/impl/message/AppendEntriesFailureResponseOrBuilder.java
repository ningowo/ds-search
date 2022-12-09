package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.AppendEntriesFailureResponseProto;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class AppendEntriesFailureResponseOrBuilder
        implements AppendEntriesFailureResponse, AppendEntriesFailureResponseBuilder, RaftMessageRequestAware {

    private AppendEntriesFailureResponseProto.Builder builder;
    private AppendEntriesFailureResponseProto response;
    private RaftEndpoint sender;

    public AppendEntriesFailureResponseOrBuilder() {
        this.builder = AppendEntriesFailureResponseProto.newBuilder();
    }

    public AppendEntriesFailureResponseOrBuilder(AppendEntriesFailureResponseProto response) {
        this.response = response;
        this.sender = RaftNodeEndpoint.wrap(response.getSender());
    }

    public AppendEntriesFailureResponseProto getResponse() {
        return response;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setAppendEntriesFailureResponse(response);
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setExpectedNextIndex(long expectedNextIndex) {
        builder.setExpectedNextIndex(expectedNextIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public long getExpectedNextIndex() {
        return response.getExpectedNextIndex();
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
            return "AppendEntriesFailureResponse{builder=" + builder + "}";
        }

        return "AppendEntriesFailureResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", "
                + "term=" + getTerm() + ", expectedNextIndex=" + getExpectedNextIndex() + ", querySequenceNumber="
                + getQuerySequenceNumber() + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + '}';
    }

}
