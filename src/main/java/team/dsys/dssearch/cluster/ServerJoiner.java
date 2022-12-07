package team.dsys.dssearch.cluster;

import cluster.internal.management.proto.*;
import cluster.internal.raft.proto.RaftEndpointProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.microraft.RaftEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.config.NodeEndpointConfig;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ServerJoiner implements Supplier<ClusterServiceImpl> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerJoiner.class);
    final ClusterServiceConfig config;
    final List<RaftEndpoint> initialMembers = new ArrayList<>();
    final Map<RaftEndpoint, String> endpointAddresses = new HashMap<>();
    final RaftEndpointProto localEndpoint;
    final boolean votingMember;

    public ServerJoiner(ClusterServiceConfig config, boolean votingMember) {
        this.config = config;
        this.localEndpoint = toProtoRaftEndpoint(config.getNodeEndpointConfig());
        this.votingMember = votingMember;
    }

    private static RaftEndpointProto toProtoRaftEndpoint(NodeEndpointConfig endpointConfig) {
        return RaftEndpointProto.newBuilder().setId(endpointConfig.getId()).build();
    }

    public ClusterServiceImpl get() {
        String joinAddress = config.getClusterConfig().getJoinTo();
        if (joinAddress == null) {
            throw new ClusterServerException("Join address is missing!");
        }

        LOGGER.debug("{} joining as {} via {}", localEndpoint.getId(),
                votingMember ? "voting member" : "non-voting member", joinAddress);

        GetRaftNodeReportResponse reportResponse = getReport(joinAddress);

        verifyReport(joinAddress, reportResponse);

        if (reportResponse.getReport().getCommittedMembers().getMemberList().contains(localEndpoint)) {
            populateDBInitState(reportResponse);

            LOGGER.warn(
                    "{} already joined to the Raft group before. Server will be created with initial "
                            + "endpoints: {} and addresses: {}",
                    localEndpoint.getId(), initialMembers, endpointAddresses);

            return new ClusterServiceImpl(config, RaftNodeEndpoint.wrap(localEndpoint), initialMembers,
                    endpointAddresses);
        }

        String localAddress = config.getNodeEndpointConfig().getAddress();
        AddRaftEndpointAddressRequest request = AddRaftEndpointAddressRequest.newBuilder()
                .setEndpoint(localEndpoint).setAddress(localAddress).build();
        //send add server request to each server in previous cluster
        //According to Microraft's documentation, before adding raftEndpoint, endpoint's address must be added to
        //every node's address info
        for (RaftEndpointProto endpoint : reportResponse.getReport().getEffectiveMembers().getMemberList()) {
            String address = reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null);
            broadcastLocalAddress(request, endpoint, address);
        }
        //send addraftendpoint request only to leader to trigger membership change
        addRaftEndpoint(reportResponse);

        LOGGER.info("{} joined to the Raft group. server is created with initial endpoints: {} and "
                + "addresses: {}", localEndpoint.getId(), initialMembers, endpointAddresses);

        return new ClusterServiceImpl(config, RaftNodeEndpoint.wrap(localEndpoint), initialMembers, endpointAddresses);
    }

    private GetRaftNodeReportResponse getReport(String joinAddress) {
        ManagedChannel reportChannel = createChannel(joinAddress);
        GetRaftNodeReportResponse reportResponse;
        try {
            reportResponse = ManagementRequestHandlerGrpc.newBlockingStub(reportChannel)
                    .getRaftNodeReport(GetRaftNodeReportRequest.getDefaultInstance());
        } finally {
            reportChannel.shutdownNow();
        }

        return reportResponse;
    }

    private ManagedChannel createChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().disableRetry().directExecutor().build();
    }

    private void verifyReport(String joinAddress, GetRaftNodeReportResponse reportResponse) {
        RaftNodeReportProto report = reportResponse.getReport();
        if (report.getStatus() != RaftNodeStatusProto.ACTIVE) {
            throw new ClusterServerException(
                    "Cannot join via " + joinAddress + " because the Raft node status is " + report.getStatus());
        }

        if (report.getEffectiveMembers().getLogIndex() != report.getCommittedMembers().getLogIndex()) {
            throw new ClusterServerException(
                    "Cannot join via " + joinAddress + " because there is another ongoing " + "membership change!");
        }

        for (RaftEndpointProto endpoint : report.getEffectiveMembers().getMemberList()) {
            if (reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null) == null) {
                throw new ClusterServerException("Cannot join via " + joinAddress + " because the address of the Raft "
                        + "endpoint: " + endpoint.getId() + " is not known!");
            }
        }

        if (report.getTerm().getLeaderEndpoint() == null) {
            throw new ClusterServerException(
                    "Cannot join via " + joinAddress + " because the Raft leader is not " + "known!");
        }
    }


    private void broadcastLocalAddress(AddRaftEndpointAddressRequest request, RaftEndpointProto target,
                                       String targetAddress) {
        LOGGER.info("{} sending local address: {} to {} at {}", localEndpoint.getId(), request.getAddress(),
                target.getId(), targetAddress);
        ManagedChannel channel = createChannel(targetAddress);
        try {
            //connect to @ManagementRequestHandlerGrpc stub to send addRaftEndpointAddress request(proto defined in @ClusterHealthManagement.proto
            ManagementRequestHandlerGrpc.newBlockingStub(channel).addRaftEndpointAddress(request);
        } catch (Throwable t) {
            throw new ClusterServerException("Could not add Raft endpoint address to " + target + " at " + targetAddress,
                    t);
        } finally {
            channel.shutdownNow();
        }
    }

    private void addRaftEndpoint(GetRaftNodeReportResponse reportResponse) {
        RaftNodeReportProto report = reportResponse.getReport();
        RaftEndpointProto leaderEndpoint = report.getTerm().getLeaderEndpoint();
        String leaderAddress = reportResponse.getEndpointAddressOrDefault(leaderEndpoint.getId(), null);
        ManagedChannel leaderChannel = createChannel(leaderAddress);
        long groupMembersCommitIndex = report.getCommittedMembers().getLogIndex();

        LOGGER.info("{} adding Raft endpoint as {} at group members commit index: {} via the Raft leader: {} at {}",
                localEndpoint.getId(), votingMember ? "voting member" : "non-voting member",
                groupMembersCommitIndex, leaderEndpoint.getId(), leaderAddress);

        AddRaftEndpointRequest request = AddRaftEndpointRequest.newBuilder().setEndpoint(localEndpoint)
                .setVotingMember(votingMember).setGroupMembersCommitIndex(groupMembersCommitIndex).build();
        try {
            //send addraftendpoint request only to leader
            ManagementRequestHandlerGrpc.newBlockingStub(leaderChannel).addRaftEndpoint(request);
        } catch (Throwable t) {
            throw new ClusterServerException(localEndpoint.getId() + " failure during add Raft endpoint via the Raft "
                    + "leader: " + leaderEndpoint + " at " + leaderAddress, t);
        } finally {
            leaderChannel.shutdownNow();
        }

        populateDBInitState(reportResponse);
    }

    private void populateDBInitState(GetRaftNodeReportResponse reportResponse) {
        for (RaftEndpointProto endpoint : reportResponse.getReport().getInitialMembers().getMemberList()) {
            initialMembers.add(RaftNodeEndpoint.wrap(endpoint));
        }

        for (RaftEndpointProto endpoint : reportResponse.getReport().getEffectiveMembers().getMemberList()) {
            String address = reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null);
            endpointAddresses.put(RaftNodeEndpoint.wrap(endpoint), address);
        }
    }

}
