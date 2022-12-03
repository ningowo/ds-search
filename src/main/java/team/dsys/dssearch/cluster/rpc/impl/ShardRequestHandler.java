package team.dsys.dssearch.cluster.rpc.impl;

import cluster.proto.*;
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
    public void put(PutRequest request, StreamObserver<ShardResponse> responseObserver) {
        LOGGER.info("SiyingChen Request" + request);
        PutOp op = PutOp.newBuilder().setKey(request.getKey()).setVal(request.getVal())
                .setPutIfAbsent(request.getPutIfAbsent()).build();
        raftNode.<PutOpResult> replicate(op).whenComplete((Ordered<PutOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setPutResult(PutResult.newBuilder().setOldVal(result.getResult().getOldVal()).build()).build());
                    } else {
                        LOGGER.error("SiyingChen error" + throwable);
                        responseObserver.onError(throwable);
                    }
                    LOGGER.info("SiyingChen info complete");
                    responseObserver.onCompleted();
                });
    }


    @Override
    public void get(GetRequest request, StreamObserver<ShardResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<GetOpResult> query(op, request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY, Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<GetOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setGetResult(
                                        GetResult.newBuilder().setVal(result.getResult().getVal()))
                                .build());
                    } else {
                        responseObserver.onError(throwable);
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<ShardResponse> responseObserver) {
        RemoveOp.Builder builder = RemoveOp.newBuilder().setKey(request.getKey());
        if (request.hasVal()) {
            builder.setVal(request.getVal());
        }
        raftNode.<RemoveOpResult> replicate(builder.build()).whenComplete((Ordered<RemoveOpResult> result, Throwable throwable) -> {
                    if (throwable == null) {
                        RemoveResult.Builder builder2 = RemoveResult
                                .newBuilder().setSuccess(result.getResult().getSuccess());
                        if (!request.hasVal() && result.getResult().hasOldVal()) {
                            builder2.setOldVal(result.getResult().getOldVal());
                        }
                        responseObserver.onNext(ShardResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setRemoveResult(builder2.build()).build());
                    } else {
                        responseObserver.onError(throwable);
                    }
                    responseObserver.onCompleted();
                });
    }



}
