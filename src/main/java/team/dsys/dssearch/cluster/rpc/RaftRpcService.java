package team.dsys.dssearch.cluster.rpc;

import io.microraft.RaftEndpoint;
import io.microraft.transport.Transport;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * responsible for sending Raft messages to other Raft nodes (serialization and networking).
 */
public interface RaftRpcService extends Transport {

    void addAddress(@Nonnull RaftEndpoint endpoint, @Nonnull String address);

    Map<RaftEndpoint, String> getAddresses();

}
