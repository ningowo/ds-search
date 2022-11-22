package team.dsys.dssearch.cluster.raft;

import io.microraft.transport.Transport;

/**
 * responsible for sending Raft messages to other Raft nodes (serialization and networking).
 */
public interface RaftRpcService extends Transport {

}
