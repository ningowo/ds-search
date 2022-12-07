package team.dsys.dssearch.cluster.raft.impl.log;

import cluster.internal.raft.proto.SnapshotEntryProto;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;

import javax.annotation.Nonnull;
import java.util.List;

public class SnapshotEntryOrBuilder implements SnapshotEntry, SnapshotEntryBuilder {

    private SnapshotEntryProto.Builder builder;
    private SnapshotEntryProto entry;
    private List<SnapshotChunk> snapshotChunks;
    private RaftGroupMembersView groupMembersView;

    public SnapshotEntryOrBuilder() {
        this.builder = SnapshotEntryProto.newBuilder();
    }

    public SnapshotEntryProto getEntry() {
        return entry;
    }

    @Override
    public long getIndex() {
        return entry.getIndex();
    }

    @Override
    public int getTerm() {
        return entry.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return snapshotChunks;
    }

    @Override
    public int getSnapshotChunkCount() {
        return entry.getSnapshotChunkCount();
    }

    @Nonnull
    @Override
    public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
    }

    @Override
    public SnapshotEntryBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Override
    public SnapshotEntryBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
        snapshotChunks.stream().map(chunk -> ((SnapshotChunkOrBuilder) chunk).getSnapshotChunk())
                .forEach(builder::addSnapshotChunk);
        this.snapshotChunks = snapshotChunks;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setGroupMembersView(RaftGroupMembersView groupMembersView) {
        builder.setGroupMembersView(((RaftGroupMembersViewOrBuilder) groupMembersView).getGroupMembersView());
        this.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntry build() {
        entry = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "SnapshotEntry{builder=" + builder + "}";
        }

        return "SnapshotEntry{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation()
                + ", groupMembersView=" + getGroupMembersView() + '}';
    }

}

