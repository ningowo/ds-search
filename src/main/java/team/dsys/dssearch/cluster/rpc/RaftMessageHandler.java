package team.dsys.dssearch.cluster.rpc;

import cluster.proto.RaftCommunicationServiceGrpc;
import cluster.proto.RaftMessageRequest;
import cluster.proto.RaftMessageResponse;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static team.dsys.dssearch.cluster.module.ClusterServiceModule.RAFT_NODE_SUPPLIER_KEY;
import static team.dsys.dssearch.cluster.rpc.utils.Serialization.unwrap;

@Singleton
public class RaftMessageHandler extends RaftCommunicationServiceGrpc.RaftCommunicationServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final RaftNode raftNode;
    private final RaftEndpoint localEndpoint;
    private final Set<RequestStreamObserver> streamObservers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Inject
    public RaftMessageHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
        this.localEndpoint = this.raftNode.getLocalEndpoint();
    }

    @Override
    public StreamObserver<RaftMessageRequest> handleRaftMessage(StreamObserver<RaftMessageResponse> responseObserver) {
        RequestStreamObserver observer = new RequestStreamObserver();
        streamObservers.add(observer);
        return observer;
    }

    private class RequestStreamObserver implements StreamObserver<RaftMessageRequest> {

        private RaftEndpoint sender;

        @Override
        public void onNext(RaftMessageRequest proto) {
            RaftMessage message = unwrap(proto);
            if (sender == null) {
                sender = message.getSender();
            }

            raftNode.handle(message);
        }

        @Override
        public void onError(Throwable t) {
            if (sender != null) {
                LOGGER.error("{} failure on Raft RPC stream of {}. Exception: {} Message: {}",
                            localEndpoint.getId(), sender.getId(), t.getClass().getSimpleName(), t.getMessage());
            } else {
                LOGGER.error(localEndpoint.getId() + " failure on Raft RPC stream. Null sender!", t);
            }
        }

        @Override
        public void onCompleted() {
            LOGGER.debug("{} Raft RPC stream of {} completed.", localEndpoint.getId(),
                    sender != null ? sender.getId() : null);
            streamObservers.remove(this);
        }

    }

}
