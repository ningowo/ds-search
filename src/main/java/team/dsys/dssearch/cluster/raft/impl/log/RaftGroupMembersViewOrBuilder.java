package team.dsys.dssearch.cluster.raft.impl.log;

import cluster.proto.RaftGroupMembersViewProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

public class RaftGroupMembersViewOrBuilder implements RaftGroupMembersView, RaftGroupMembersViewBuilder {

    private RaftGroupMembersViewProto.Builder builder;
    private RaftGroupMembersViewProto groupMembersView;
    private Collection<RaftEndpoint> members = new LinkedHashSet<>();
    private Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>();

    public RaftGroupMembersViewOrBuilder() {
        this.builder = RaftGroupMembersViewProto.newBuilder();
    }

    public RaftGroupMembersViewOrBuilder(RaftGroupMembersViewProto groupMembersView) {
        this.groupMembersView = groupMembersView;
        groupMembersView.getMemberList().stream().map(RaftNodeEndpoint::wrap).forEach(members::add);
        groupMembersView.getVotingMemberList().stream().map(RaftNodeEndpoint::wrap).forEach(votingMembers::add);
    }

    public RaftGroupMembersViewProto getGroupMembersView() {
        return groupMembersView;
    }

    @Override
    public long getLogIndex() {
        return groupMembersView.getLogIndex();
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getMembers() {
        return Collections.unmodifiableCollection(members);
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getVotingMembers() {
        return Collections.unmodifiableCollection(votingMembers);
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder setLogIndex(long logIndex) {
        builder.setLogIndex(logIndex);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        members.stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addMember);
        this.members.clear();
        this.members.addAll(members);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder setVotingMembers(@Nonnull Collection<RaftEndpoint> votingMembers) {
        votingMembers.stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addVotingMember);
        this.votingMembers.clear();
        this.votingMembers.addAll(votingMembers);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersView build() {
        groupMembersView = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "RaftGroupMembersView{builder=" + builder + "}";
        }

        return "RaftGroupMembersView{" + "logIndex=" + getLogIndex() + ", members=" + getMembers() + ", votingMembers="
                + getVotingMembers() + '}';
    }

}
