package team.dsys.dssearch.cluster.rpc.impl;

import cluster.internal.management.proto.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;
import team.dsys.dssearch.cluster.rpc.RaftRpcService;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.RAFT_NODE_SUPPLIER_KEY;
import static team.dsys.dssearch.cluster.rpc.utils.Serialization.toProto;

/**
 * Implement @ManagementRequestHandlerGrpc defined in @ClusterHealthManagement.proto
 */
@Singleton
public class ClusterHealthManagementHandler extends ManagementRequestHandlerGrpc.ManagementRequestHandlerImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterHealthManagementHandler.class);

    private final RaftNode raftNode;
    private final RaftRpcService raftRpcService;

    @Inject
    public ClusterHealthManagementHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier,
                                    RaftRpcService raftRpcService) {
        this.raftNode = raftNodeSupplier.get();
        this.raftRpcService = raftRpcService;
    }


    public void removeRaftEndpoint() {
        //to-do, membership change: for removing a node from cluster
    }

    @Override
    public void getRaftNodeReport(GetRaftNodeReportRequest request,
                                  StreamObserver<GetRaftNodeReportResponse> responseObserver) {
        raftNode.getReport().whenComplete((response, throwable) -> {
            if (throwable == null) {
                GetRaftNodeReportResponse.Builder builder = GetRaftNodeReportResponse.newBuilder();
                builder.setReport(toProto(response.getResult()));
                raftRpcService.getAddresses()
                        .forEach((key, value) -> builder.putEndpointAddress(key.getId().toString(), value));

                responseObserver.onNext(builder.build());
            } else {
                responseObserver.onError(throwable);
            }
            responseObserver.onCompleted();
        });
    }

    /**
     * Add a raftendpoint to target node's address
     * @param request see in @ClusterHealthManagement.proto
     * @param responseObserver
     */
    @Override
    public void addRaftEndpointAddress(AddRaftEndpointAddressRequest request,
                                       StreamObserver<AddRaftEndpointAddressResponse> responseObserver) {
        try {
            RaftEndpoint endpoint = RaftNodeEndpoint.wrap(request.getEndpoint());
            LOGGER.info("Adding address: {} for {}.", request.getAddress(), endpoint);
            raftRpcService.addAddress(endpoint, request.getAddress());
            responseObserver.onNext(AddRaftEndpointAddressResponse.getDefaultInstance());
        } catch (Throwable t) {
            responseObserver.onError(t);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void addRaftEndpoint(AddRaftEndpointRequest request,
                                StreamObserver<AddRaftEndpointResponse> responseObserver) {
        RaftEndpoint endpoint = RaftNodeEndpoint.wrap(request.getEndpoint());
        String address = raftRpcService.getAddresses().get(endpoint);
        if (address == null) {
            LOGGER.error("{} cannot add {} because its address is not known!", raftNode.getLocalEndpoint().getId(),
                    endpoint.getId());
            responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION));
            responseObserver.onCompleted();
            return;
        }

        //in microraft setting, adding learner role
        MembershipChangeMode mode = request.getVotingMember() ? MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER
                : MembershipChangeMode.ADD_LEARNER;

        LOGGER.info("{} is adding {} with mode: {} and address: {}.", raftNode.getLocalEndpoint().getId(),
                endpoint.getId(), mode, address);
        //trigger membership change in microraft
        raftNode.changeMembership(endpoint, mode, request.getGroupMembersCommitIndex())
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        long newCommitIndex = result.getCommitIndex();
                        LOGGER.info("{} added {} with address: {}.", raftNode.getLocalEndpoint().getId(),
                                endpoint.getId(), address);
                        AddRaftEndpointResponse response = AddRaftEndpointResponse.newBuilder()
                                .setGroupMembersCommitIndex(newCommitIndex).build();
                        responseObserver.onNext(response);
                    } else {
                        LOGGER.error(
                                raftNode.getLocalEndpoint().getId() + " could not add " + endpoint + " "
                                        + "with group members commit index: " + request.getGroupMembersCommitIndex(),
                                throwable);
                        responseObserver.onError(throwable);
                    }
                    responseObserver.onCompleted();
                });
    }

}

