package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.AppendEntriesRequestProto;
import cluster.internal.raft.proto.LogEntryProto;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesRequest;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;
import team.dsys.dssearch.cluster.raft.impl.log.LogEntryOrBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesRequestOrBuilder
        implements AppendEntriesRequest, AppendEntriesRequest.AppendEntriesRequestBuilder, RaftMessageRequestAware {

    private AppendEntriesRequestProto.Builder builder;
    private AppendEntriesRequestProto request;
    private RaftEndpoint sender;
    private List<LogEntry> logEntries;

    public AppendEntriesRequestOrBuilder() {
        builder = AppendEntriesRequestProto.newBuilder();
    }

    public AppendEntriesRequestOrBuilder(AppendEntriesRequestProto request) {
        this.request = request;
        this.sender = RaftNodeEndpoint.wrap(request.getSender());
        this.logEntries = new ArrayList<>(request.getEntryCount());
        for (LogEntryProto e : request.getEntryList()) {
            this.logEntries.add(new LogEntryOrBuilder(e));
        }
    }

    public AppendEntriesRequestProto getRequest() {
        return request;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setAppendEntriesRequest(request);
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogTerm(int previousLogTerm) {
        builder.setPrevLogTerm(previousLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogIndex(long previousLogIndex) {
        builder.setPrevLogIndex(previousLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setCommitIndex(long commitIndex) {
        builder.setCommitIndex(commitIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries) {
        for (LogEntry entry : logEntries) {
            builder.addEntry(((LogEntryOrBuilder) entry).getEntry());
        }

        this.logEntries = logEntries;

        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public int getPreviousLogTerm() {
        return request.getPrevLogTerm();
    }

    @Override
    public long getPreviousLogIndex() {
        return request.getPrevLogIndex();
    }

    @Override
    public long getCommitIndex() {
        return request.getCommitIndex();
    }

    @Nonnull
    @Override
    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    public long getQuerySequenceNumber() {
        return request.getQuerySequenceNumber();
    }

    @Override
    public long getFlowControlSequenceNumber() {
        return request.getFlowControlSequenceNumber();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "AppendEntriesRequest{builder=" + builder + "}";
        }

        return "AppendEntriesRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term="
                + getTerm() + ", commitIndex=" + getCommitIndex() + ", querySequenceNumber=" + getQuerySequenceNumber()
                + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + ", " + "prevLogIndex="
                + getPreviousLogIndex() + ", prevLogTerm=" + getPreviousLogTerm() + ", entries=" + getLogEntries()
                + '}';
    }

}