package team.dsys.dssearch.cluster.raft.model;

import io.microraft.model.RaftModelFactory;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;
import io.microraft.model.message.AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import io.microraft.model.message.TriggerLeaderElectionRequest.TriggerLeaderElectionRequestBuilder;
import io.microraft.model.message.VoteRequest.VoteRequestBuilder;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;
import team.dsys.dssearch.cluster.raft.impl.group.UpdateRaftGroupMembersOpOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.log.LogEntryOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.log.RaftGroupMembersViewOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.log.SnapshotChunkOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.log.SnapshotEntryOrBuilder;
import team.dsys.dssearch.cluster.raft.impl.message.*;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

@Singleton
public class RaftModelFactoryImpl implements RaftModelFactory {

    @Nonnull
    @Override
    public LogEntryBuilder createLogEntryBuilder() {
        return new LogEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder createSnapshotEntryBuilder() {
        return new SnapshotEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotChunk.SnapshotChunkBuilder createSnapshotChunkBuilder() {
        return new SnapshotChunkOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
        return new AppendEntriesRequestOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
        return new AppendEntriesSuccessResponseOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
        return new AppendEntriesFailureResponseOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
        return new InstallSnapshotRequestOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
        return new InstallSnapshotResponseOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder createPreVoteRequestBuilder() {
        return new PreVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder createPreVoteResponseBuilder() {
        return new PreVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
        return new TriggerLeaderElectionRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteRequestBuilder createVoteRequestBuilder() {
        return new VoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteResponseBuilder createVoteResponseBuilder() {
        return new VoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
        return new UpdateRaftGroupMembersOpOrBuilder();
    }

    @Override
    public RaftGroupMembersViewBuilder createRaftGroupMembersViewBuilder() {
        return new RaftGroupMembersViewOrBuilder();
    }

}

