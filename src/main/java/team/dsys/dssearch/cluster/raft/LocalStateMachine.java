package team.dsys.dssearch.cluster.raft;

import io.microraft.statemachine.StateMachine;

import java.util.List;
import java.util.function.Consumer;

public class LocalStateMachine implements StateMachine {
    /**
     * This defines how the local state machine executes an operation
     * when the task is committed, i.e., replicated to a majority of nodes.
     * @param commitIndex
     *            the Raft log index on which the given operation is committed
     * @param operation
     *             the user-supplied operation to be executed
     * @return the result of the operation execution
     */
    @Override
    public Object runOperation(long commitIndex, Object operation) {
        //todo
        //operation: update status, update shard info
        return null;
    }

    @Override
    public void takeSnapshot(long l, Consumer<Object> consumer) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void installSnapshot(long l, List<Object> list) {
        throw new UnsupportedOperationException();

    }

    @Override
    public Object getNewTermOperation() {
        //This method returns an operation which will be committed every time a new leader is elected.
        // This is actually related to the single-server membership change bug in the Raft consensus algorithm.
        return new NewTermOperation();
    }

    private static class NewTermOperation {
        //todo:
    }
}
