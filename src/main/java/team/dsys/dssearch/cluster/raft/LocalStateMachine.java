package team.dsys.dssearch.cluster.raft;

import cluster.external.shard.proto.ShardInfo;
import cluster.external.shard.proto.ShardInfoWithDataNodeInfo;
import cluster.internal.raft.proto.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.microraft.RaftEndpoint;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.NODE_ENDPOINT_KEY;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStateMachine.class);
    //in-memory map to keep shard status and node status
    // use linkedhash map to keep the insertion order
    //Key: shardId, value: list of shardInfo(isPrimary) and its DataNodeInfo(list of datanodeId and address)
    private final Map<ShardInfo, ShardInfoWithDataNodeInfo> map = new ConcurrentHashMap<>();

    private final RaftEndpoint raftNodeEndpoint;

    @Inject
    public LocalStateMachine(@Named(NODE_ENDPOINT_KEY) RaftEndpoint raftEndpoint) {
        this.raftNodeEndpoint = raftEndpoint;
    }

    /**
     * Microraft will finally call runOperation on every node when applying entries
     * @param commitIndex
     * @param operation
     * @return
     */
    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        requireNonNull(operation);
        if (operation instanceof PutOp) {
            return put(commitIndex, (PutOp) operation);
        } else if (operation instanceof GetOp) {
            return get(commitIndex, (GetOp) operation);
        } else if (operation instanceof GetAllOp) {
            return getAll(commitIndex, (GetAllOp) operation);
        } else if (operation instanceof StartNewTermOpProto) {
            return null;
        }

        throw new IllegalArgumentException("Invalid operation: " + operation + " of class: " + operation.getClass()
                + " at commit index: " + commitIndex);
    }

    //for testing
    private void printMap(String info) {
        System.out.println(info);
        for(Map.Entry<ShardInfo, ShardInfoWithDataNodeInfo> e : map.entrySet()) {
            System.out.println("ShardInfo: " + e.getKey() + " ----  map value: " + e.getValue());
        }
    }

    //do local put operation
    private PutOpResult put(long commitIndex, PutOp op) {
        List<ShardInfo> shardInfoList = op.getShardInfoList();
        for (ShardInfo shardInfo : shardInfoList) {
            if (!map.containsKey(shardInfo)) {
                map.put(shardInfo, ShardInfoWithDataNodeInfo.newBuilder().setShardInfo(shardInfo).build());
            }

            ShardInfoWithDataNodeInfo.Builder builder = map.get(shardInfo).toBuilder();
            if (builder.getDataNodeInfosList() != null
                    && builder.getDataNodeInfosList().contains(op.getDataNodeInfo())) {
                continue;
            }
            map.put(shardInfo, builder.addDataNodeInfos(op.getDataNodeInfo()).build());

        }

        PutOpResult.Builder builder = PutOpResult.newBuilder().setStatus(0).setMsg("PutShardRequest Success");

        return builder.build();
    }

    private GetOpResult get(long commitIndex, GetOp op) {
        List<Integer> shardIdList = op.getShardIdList();
        List<ShardInfo> shardInfoList = new ArrayList<>();
        for (Integer shardId : shardIdList) {
            ShardInfo primaryShardInfo = ShardInfo.newBuilder().setShardId(shardId).setIsPrimary(true).build();
            ShardInfo nonPrimaryShardInfo = ShardInfo.newBuilder().setShardId(shardId).setIsPrimary(false).build();
            shardInfoList.add(primaryShardInfo);
            shardInfoList.add(nonPrimaryShardInfo);
        }

        List<ShardInfoWithDataNodeInfo> shardInfoWithDataNodeInfoList = new ArrayList<>();
        for (ShardInfo shardInfo : shardInfoList) {
            ShardInfoWithDataNodeInfo shardInfoWithDataNodeInfo = map.get(shardInfo);
            if (shardInfoWithDataNodeInfo != null) {
                shardInfoWithDataNodeInfoList.add(shardInfoWithDataNodeInfo);
            }

        }

        GetOpResult.Builder builder = GetOpResult.newBuilder().addAllShardInfoWithDataNodeInfo(shardInfoWithDataNodeInfoList);

        return builder.build();
    }

    private GetOpResult getAll(long commitIndex, GetAllOp op) {
        List<ShardInfoWithDataNodeInfo> shardInfoWithDataNodeInfoList = new ArrayList<>();
        for(Map.Entry<ShardInfo, ShardInfoWithDataNodeInfo> e : map.entrySet()) {
            shardInfoWithDataNodeInfoList.add(e.getValue());
        }

        GetOpResult.Builder builder = GetOpResult.newBuilder().addAllShardInfoWithDataNodeInfo(shardInfoWithDataNodeInfoList);

        return builder.build();

    }


    //create snapshot chunk data
    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        ClusterSnapshotChunkData.Builder chunkBuilder = ClusterSnapshotChunkData.newBuilder();

        int chunkCount = 0, keyCount = 0;
        for (Map.Entry<ShardInfo, ShardInfoWithDataNodeInfo> e : map.entrySet()) {
            keyCount++;
            ShardEntry shardEntry = ShardEntry.newBuilder().setShardInfo(e.getKey()).setShardInfoWithDataNodeInfo(e.getValue()).build();
            chunkBuilder.addEntry(shardEntry);
            if (chunkBuilder.getEntryCount() == 10000) {
                snapshotChunkConsumer.accept(chunkBuilder.build());
                chunkBuilder = ClusterSnapshotChunkData.newBuilder();
                chunkCount++;
            }
        }

        if (map.size() == 0 || chunkBuilder.getEntryCount() > 0) {
            snapshotChunkConsumer.accept(chunkBuilder.build());
            chunkCount++;
        }

        LOGGER.info("{} took snapshot with {} chunks and {} keys at log index: {}", raftNodeEndpoint.getId(), chunkCount,
                keyCount, commitIndex);
    }

    //install snapshot chunk data
    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        map.clear();

        for (Object chunk : snapshotChunks) {
            for (ShardEntry entry : ((ClusterSnapshotChunkData) chunk).getEntryList()) {
                map.put(entry.getShardInfo(), entry.getShardInfoWithDataNodeInfo());
            }
        }

        LOGGER.info("{} restored snapshot with {} keys at commit index: {}", raftNodeEndpoint.getId(), map.size(),
                commitIndex);
    }

    @Nonnull
    @Override
    public Object getNewTermOperation() {
        return StartNewTermOpProto.getDefaultInstance();
    }

}
