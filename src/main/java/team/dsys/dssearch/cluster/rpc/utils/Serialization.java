package team.dsys.dssearch.cluster.rpc.utils;

import cluster.internal.management.proto.*;
import cluster.internal.raft.proto.RaftMessageRequest;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;
import io.microraft.report.RaftTerm;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;
import team.dsys.dssearch.cluster.raft.impl.message.*;

import javax.annotation.Nonnull;

public final class Serialization {

    private Serialization() {
    }

    public static RaftNodeReportProto toProto(RaftNodeReport report) {
        return RaftNodeReportProto.newBuilder().setReason(toProto(report.getReason()))
                .setGroupId((String) report.getGroupId()).setEndpoint(RaftNodeEndpoint.unwrap(report.getEndpoint()))
                .setInitialMembers(toProto(report.getInitialMembers()))
                .setCommittedMembers(toProto(report.getCommittedMembers()))
                .setEffectiveMembers(toProto(report.getEffectiveMembers())).setRole(toProto(report.getRole()))
                .setStatus(toProto(report.getStatus())).setTerm(toProto(report.getTerm()))
                .setLog(toProto(report.getLog())).build();
    }

    public static RaftNodeReportReasonProto toProto(RaftNodeReportReason reason) {
        switch (reason) {
            case STATUS_CHANGE:
                return RaftNodeReportReasonProto.STATUS_CHANGE;
            case ROLE_CHANGE:
                return RaftNodeReportReasonProto.ROLE_CHANGE;
            case GROUP_MEMBERS_CHANGE:
                return RaftNodeReportReasonProto.GROUP_MEMBERS_CHANGE;
            case TAKE_SNAPSHOT:
                return RaftNodeReportReasonProto.TAKE_SNAPSHOT;
            case INSTALL_SNAPSHOT:
                return RaftNodeReportReasonProto.INSTALL_SNAPSHOT;
            case PERIODIC:
                return RaftNodeReportReasonProto.PERIODIC;
            case API_CALL:
                return RaftNodeReportReasonProto.API_CALL;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeReportReason: " + reason);
        }
    }

    public static RaftGroupMembersProto toProto(RaftGroupMembers groupMembers) {
        RaftGroupMembersProto.Builder builder = RaftGroupMembersProto.newBuilder();
        builder.setLogIndex(groupMembers.getLogIndex());

        groupMembers.getMembers().stream().map(RaftNodeEndpoint::unwrap).forEach(builder::addMember);

        return builder.build();
    }

    public static RaftRoleProto toProto(RaftRole role) {
        switch (role) {
            case LEADER:
                return RaftRoleProto.LEADER;
            case CANDIDATE:
                return RaftRoleProto.CANDIDATE;
            case FOLLOWER:
                return RaftRoleProto.FOLLOWER;
            case LEARNER:
                return RaftRoleProto.LEARNER;

            default:
                throw new IllegalArgumentException("Invalid RaftRole: " + role);
        }
    }

    public static RaftNodeStatusProto toProto(RaftNodeStatus status) {
        switch (status) {
            case INITIAL:
                return RaftNodeStatusProto.INITIAL;
            case ACTIVE:
                return RaftNodeStatusProto.ACTIVE;
            case UPDATING_RAFT_GROUP_MEMBER_LIST:
                return RaftNodeStatusProto.UPDATING_RAFT_GROUP_MEMBER_LIST;
            case TERMINATED:
                return RaftNodeStatusProto.TERMINATED;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeStatus: " + status);
        }
    }

    public static RaftTermProto toProto(RaftTerm term) {
        RaftTermProto.Builder builder = RaftTermProto.newBuilder();

        builder.setTerm(term.getTerm());

        if (term.getLeaderEndpoint() != null) {
            builder.setLeaderEndpoint(RaftNodeEndpoint.unwrap(term.getLeaderEndpoint()));
        }

        if (term.getVotedEndpoint() != null) {
            builder.setVotedEndpoint(RaftNodeEndpoint.unwrap(term.getVotedEndpoint()));
        }

        return builder.build();
    }

    public static RaftLogStatsProto toProto(RaftLogStats log) {
        RaftLogStatsProto.Builder builder = RaftLogStatsProto.newBuilder();
        builder.setCommitIndex(log.getCommitIndex()).setLastLogOrSnapshotIndex(log.getLastLogOrSnapshotIndex())
                .setLastLogOrSnapshotTerm(log.getLastLogOrSnapshotTerm()).setSnapshotIndex(log.getLastSnapshotIndex())
                .setSnapshotTerm(log.getLastSnapshotTerm()).setTakeSnapshotCount(log.getTakeSnapshotCount())
                .setInstallSnapshotCount(log.getInstallSnapshotCount());

        log.getFollowerMatchIndices()
                .forEach((key, value) -> builder.putFollowerMatchIndex(key.getId().toString(), value));

        return builder.build();
    }

    public static RaftMessage unwrap(@Nonnull RaftMessageRequest request) {
        switch (request.getMessageCase()) {
            case VOTEREQUEST:
                return new VoteRequestOrBuilder(request.getVoteRequest());
            case VOTERESPONSE:
                return new VoteResponseOrBuilder(request.getVoteResponse());
            case APPENDENTRIESREQUEST:
                return new AppendEntriesRequestOrBuilder(request.getAppendEntriesRequest());
            case APPENDENTRIESSUCCESSRESPONSE:
                return new AppendEntriesSuccessResponseOrBuilder(request.getAppendEntriesSuccessResponse());
            case APPENDENTRIESFAILURERESPONSE:
                return new AppendEntriesFailureResponseOrBuilder(request.getAppendEntriesFailureResponse());
            case INSTALLSNAPSHOTREQUEST:
                return new InstallSnapshotRequestOrBuilder(request.getInstallSnapshotRequest());
            case INSTALLSNAPSHOTRESPONSE:
                return new InstallSnapshotResponseOrBuilder(request.getInstallSnapshotResponse());
            case PREVOTEREQUEST:
                return new PreVoteRequestOrBuilder(request.getPreVoteRequest());
            case PREVOTERESPONSE:
                return new PreVoteResponseOrBuilder(request.getPreVoteResponse());
            case TRIGGERLEADERELECTIONREQUEST:
                return new TriggerLeaderElectionRequestOrBuilder(request.getTriggerLeaderElectionRequest());
            default:
                throw new IllegalArgumentException("Invalid request: " + request);
        }
    }

    public static RaftMessageRequest wrap(@Nonnull RaftMessage message) {
        RaftMessageRequest.Builder builder = RaftMessageRequest.newBuilder();
        if (message instanceof RaftMessageRequestAware) {
            ((RaftMessageRequestAware) message).populate(builder);
        } else {
            throw new IllegalArgumentException("Cannot convert " + message + " to proto");
        }

        return builder.build();
    }

}



