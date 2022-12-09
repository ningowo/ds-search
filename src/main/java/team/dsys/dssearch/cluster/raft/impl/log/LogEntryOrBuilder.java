package team.dsys.dssearch.cluster.raft.impl.log;

import cluster.internal.raft.proto.*;
import io.microraft.model.log.LogEntry;
import team.dsys.dssearch.cluster.raft.impl.group.UpdateRaftGroupMembersOpOrBuilder;

import javax.annotation.Nonnull;

public class LogEntryOrBuilder implements LogEntry, LogEntry.LogEntryBuilder {
    private LogEntryProto.Builder builder;
    private LogEntryProto entry;

    public LogEntryOrBuilder() {
        this.builder = LogEntryProto.newBuilder();
    }

    public LogEntryOrBuilder(LogEntryProto entry) {
        this.entry = entry;
    }

    public LogEntryProto getEntry() {
        return entry;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setOperation(@Nonnull Object operation) {
        if (operation instanceof UpdateRaftGroupMembersOpOrBuilder) {
            builder.setUpdateRaftGroupMembersOp(((UpdateRaftGroupMembersOpOrBuilder) operation).getOp());
        } else if (operation instanceof StartNewTermOpProto) {
            builder.setStartNewTermOp((StartNewTermOpProto) operation);
            //todo: other list of operation(update status, shard...)
        } else if (operation instanceof PutOp) {
                builder.setPutOp((PutOp) operation);
        } else if (operation instanceof GetOp) {
                builder.setGetOp((GetOp) operation);
        } else {
            throw new IllegalArgumentException("Invalid operation: " + operation);
        }

        return this;
    }

    @Nonnull
    @Override
    public LogEntry build() {
        entry = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "LogEntry{builder=" + builder + "}";
        }

        return "LogEntry{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation() + '}';
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
        switch (entry.getOperationCase()) {
            case UPDATERAFTGROUPMEMBERSOP:
                return new UpdateRaftGroupMembersOpOrBuilder(entry.getUpdateRaftGroupMembersOp());
            case STARTNEWTERMOP:
                return entry.getStartNewTermOp();
            //todo: other list of operation(update status, shard...)
            case PUTOP:
                return entry.getPutOp();
            case GETOP:
                return entry.getGetOp();
            default:
                throw new IllegalArgumentException("Illegal operation in " + entry);
        }
    }

}
