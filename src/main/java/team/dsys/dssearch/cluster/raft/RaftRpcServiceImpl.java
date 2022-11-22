package team.dsys.dssearch.cluster.raft;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftRpcServiceImpl implements RaftRpcService{
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcService.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> nodeAddresses;
    private final Map<RaftEndpoint, grpcStub> stubs = new ConcurrentHashMap<RaftEndpoint, grpcStub>();

    public RaftRpcServiceImpl(RaftEndpoint localEndpoint, Map<RaftEndpoint, String> nodeAddresses) {
        this.localEndpoint = localEndpoint;
        this.nodeAddresses = nodeAddresses;
    }


    @Override
    public void send(RaftEndpoint target, RaftMessage raftMessage) {
        //check if target endpoint is valid
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send message to itself...", localEndpoint.getId());
            return null;
        }

        if (!nodeAddresses.containsKey(target)) {
            LOGGER.error("Unknown target endpoint: {}", target);
            return null;
        }

        //get or create stub
        grpcStub stub = stubs.get(target);
        if (grpcStub != null) {

        }

        return






    }

    @Override
    public boolean isReachable(RaftEndpoint raftEndpoint) {
        return false;
    }

}
