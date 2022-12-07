package team.dsys.dssearch.cluster.raft.impl.group;

import cluster.internal.raft.proto.UpdateRaftGroupMembersOpProto;
import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateRaftGroupMembersOpOrBuilder implements UpdateRaftGroupMembersOp, UpdateRaftGroupMembersOpBuilder {

    private UpdateRaftGroupMembersOpProto.Builder builder;
    private UpdateRaftGroupMembersOpProto op;
    private Collection<RaftEndpoint> members = new LinkedHashSet<>();
    private Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>();
    private RaftEndpoint endpoint;

    public UpdateRaftGroupMembersOpOrBuilder() {
        this.builder = UpdateRaftGroupMembersOpProto.newBuilder();
    }

    public UpdateRaftGroupMembersOpOrBuilder(UpdateRaftGroupMembersOpProto op) {
        this.op = op;
        op.getMemberList().stream().map(RaftNodeEndpoint::wrap).forEach(members::add);
        op.getVotingMemberList().stream().map(RaftNodeEndpoint::wrap).forEach(votingMembers::add);
        this.endpoint = RaftNodeEndpoint.wrap(op.getEndpoint());
    }

    public UpdateRaftGroupMembersOpProto getOp() {
        return op;
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
    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    @Nonnull
    @Override
    public MembershipChangeMode getMode() {
        if (op.getMode() == UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.ADD_LEARNER) {
            return MembershipChangeMode.ADD_LEARNER;
        } else if (op.getMode() == UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.ADD_OR_PROMOTE_TO_FOLLOWER) {
            return MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
        } else if (op.getMode() == UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.REMOVE_MEMBER) {
            return MembershipChangeMode.REMOVE_MEMBER;
        }
        throw new IllegalStateException();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        members.stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addMember);
        this.members.clear();
        this.members.addAll(members);
        return this;
    }

    @Override
    public UpdateRaftGroupMembersOpBuilder setVotingMembers(Collection<RaftEndpoint> votingMembers) {
        members.stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addVotingMember);
        this.votingMembers.clear();
        this.votingMembers.addAll(votingMembers);
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint) {
        builder.setEndpoint(RaftNodeEndpoint.unwrap(endpoint));
        this.endpoint = endpoint;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode) {
        if (mode == MembershipChangeMode.ADD_LEARNER) {
            builder.setMode(UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.ADD_LEARNER);
            return this;
        } else if (mode == MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER) {
            builder.setMode(UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.ADD_OR_PROMOTE_TO_FOLLOWER);
            return this;
        } else if (mode == MembershipChangeMode.REMOVE_MEMBER) {
            builder.setMode(UpdateRaftGroupMembersOpProto.MembershipChangeModeProto.REMOVE_MEMBER);
            return this;
        }

        throw new ClusterServerException("Invalid mode: " + mode);
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOp build() {
        op = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "UpdateRaftGroupMembersOp{builder=" + builder + "}";
        }

        List<Object> memberIds = members.stream().map(RaftEndpoint::getId).collect(Collectors.toList());
        List<Object> votingMemberIds = votingMembers.stream().map(RaftEndpoint::getId).collect(Collectors.toList());
        return "UpdateRaftGroupMembersOp{" + "members=" + memberIds + ", votingMembers=" + votingMemberIds
                + ", endpoint=" + endpoint.getId() + ", " + "mode=" + getMode() + '}';
    }

}