package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.internal.raft.proto.ClusterSnapshotChunk;
import cluster.internal.raft.proto.InstallSnapshotRequestProto;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;
import team.dsys.dssearch.cluster.raft.impl.log.RaftGroupMembersViewOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.log.SnapshotChunkOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static java.util.stream.Collectors.toList;

public class InstallSnapshotRequestOrBuilder implements InstallSnapshotRequest,
        InstallSnapshotRequest.InstallSnapshotRequestBuilder, RaftMessageRequestAware {

    private InstallSnapshotRequestProto.Builder builder;
    private InstallSnapshotRequestProto request;
    private RaftEndpoint sender;
    private SnapshotChunk snapshotChunk;
    private Collection<RaftEndpoint> snapshottedMembers;
    private RaftGroupMembersView groupMembersView;

    public InstallSnapshotRequestOrBuilder() {
        this.builder = InstallSnapshotRequestProto.newBuilder();
    }

    public InstallSnapshotRequestOrBuilder(InstallSnapshotRequestProto request) {
        this.request = request;
        this.sender = RaftNodeEndpoint.wrap(request.getSender());
        if (!request.getSnapshotChunk().equals(ClusterSnapshotChunk.getDefaultInstance())) {
            this.snapshotChunk = new SnapshotChunkOrBuilder(request.getSnapshotChunk());
        }
        this.snapshottedMembers = request.getSnapshottedMemberList().stream().map(RaftNodeEndpoint::wrap)
                .collect(toList());
        this.groupMembersView = new RaftGroupMembersViewOrBuilder(request.getGroupMembersView());
    }

    public InstallSnapshotRequestProto getRequest() {
        return request;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSenderLeader(boolean leader) {
        builder.setSenderLeader(leader);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotTerm(int snapshotTerm) {
        builder.setSnapshotTerm(snapshotTerm);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotIndex(long snapshotIndex) {
        builder.setSnapshotIndex(snapshotIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(int totalSnapshotChunkCount) {
        builder.setTotalSnapshotChunkCount(totalSnapshotChunkCount);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotChunk(@Nullable SnapshotChunk snapshotChunk) {
        if (snapshotChunk != null) {
            builder.setSnapshotChunk(((SnapshotChunkOrBuilder) snapshotChunk).getSnapshotChunk());
        }

        this.snapshotChunk = snapshotChunk;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshottedMembers(@Nonnull Collection<RaftEndpoint> snapshottedMembers) {
        snapshottedMembers.stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addSnapshottedMember);
        this.snapshottedMembers = snapshottedMembers;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupMembersView(RaftGroupMembersView groupMembersView) {
        builder.setGroupMembersView(((RaftGroupMembersViewOrBuilder) groupMembersView).getGroupMembersView());
        this.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setInstallSnapshotRequest(request);
    }

    @Override
    public boolean isSenderLeader() {
        return request.getSenderLeader();
    }

    @Override
    public int getSnapshotTerm() {
        return request.getSnapshotTerm();
    }

    @Override
    public long getSnapshotIndex() {
        return request.getSnapshotIndex();
    }

    @Override
    public int getTotalSnapshotChunkCount() {
        return request.getTotalSnapshotChunkCount();
    }

    @Nullable
    @Override
    public SnapshotChunk getSnapshotChunk() {
        return snapshotChunk;
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getSnapshottedMembers() {
        return snapshottedMembers;
    }

    @Nonnull
    @Override
    public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
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
            return "InstallSnapshotRequest{builder=" + builder + "}";
        }

        return "InstallSnapshotRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term="
                + getTerm() + ", senderLeader=" + isSenderLeader() + ", snapshotTerm=" + getSnapshotTerm()
                + ", snapshotIndex=" + getSnapshotIndex() + ", chunkCount=" + getTotalSnapshotChunkCount()
                + ", snapshotChunk=" + getSnapshotChunk() + ", snapshottedMembers=" + getSnapshottedMembers()
                + ", groupMembersView=" + getGroupMembersView() + ", querySequenceNumber=" + getQuerySequenceNumber()
                + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + '}';
    }

}
