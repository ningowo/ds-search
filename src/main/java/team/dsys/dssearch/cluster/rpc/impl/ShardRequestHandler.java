package team.dsys.dssearch.cluster.rpc.impl;


import cluster.external.shard.proto.*;
import cluster.internal.raft.proto.*;
import io.grpc.stub.StreamObserver;
import io.microraft.Ordered;
import io.microraft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.RAFT_NODE_SUPPLIER_KEY;

@Singleton
public class ShardRequestHandler extends ShardRequestHandlerGrpc.ShardRequestHandlerImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardRequestHandler.class);

    private final RaftNode raftNode;

    @Inject
    public ShardRequestHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
    }

    @Override
    public void put(PutShardRequest request, StreamObserver<ShardResponse> responseObserver) {
        PutOp op = PutOp.newBuilder().setDataNodeInfo(request.getDataNodeInfo()).addAllShardInfo(request.getShardInfoList()).build();
        raftNode.<PutOpResult> replicate(op).whenComplete((Ordered<PutOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setCommonResponse(CommonResponse.newBuilder().setStatus(result.getResult().getStatus()).setMsg(result.getResult().getMsg())).build());
                    } else {
                        LOGGER.error(throwable.getMessage());
                        responseObserver.onError(throwable);
                    }
                    LOGGER.info("complete");
                    responseObserver.onCompleted();
                });
    }


    @Override
    public void get(GetShardRequest request, StreamObserver<ShardResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().addAllShardId(request.getShardIdList()).build();
        raftNode.<GetOpResult> query(op, request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY, Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<GetOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setGetShardResponse(
                                        GetShardResponse.newBuilder().addAllShardInfoWithDataNodeInfo(result.getResult().getShardInfoWithDataNodeInfoList()).build())
                                .build());
                    } else {
                        responseObserver.onError(throwable);
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void getAll(GetAllShardRequest request, StreamObserver<ShardResponse> responseObserver) {
        GetAllOp op = GetAllOp.newBuilder().build();
        raftNode.<GetOpResult> query(op, LINEARIZABLE, 0)
                .whenComplete((Ordered<GetOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setGetShardResponse(
                                        GetShardResponse.newBuilder().addAllShardInfoWithDataNodeInfo(result.getResult().getShardInfoWithDataNodeInfoList()).build())
                                .build());
                    } else {
                        responseObserver.onError(throwable);
                    }
                    responseObserver.onCompleted();
                });
    }
}
