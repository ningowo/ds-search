package team.dsys.dssearch.cluster.raft.impl.message;

import cluster.proto.RaftMessageRequest;
import cluster.proto.TriggerLeaderElectionRequestProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.TriggerLeaderElectionRequest;
import io.microraft.model.message.TriggerLeaderElectionRequest.TriggerLeaderElectionRequestBuilder;
import team.dsys.dssearch.cluster.raft.RaftNodeEndpoint;

import javax.annotation.Nonnull;

public class TriggerLeaderElectionRequestOrBuilder
        implements TriggerLeaderElectionRequest, TriggerLeaderElectionRequestBuilder, RaftMessageRequestAware {

    private TriggerLeaderElectionRequestProto.Builder builder;
    private TriggerLeaderElectionRequestProto request;
    private RaftEndpoint sender;

    public TriggerLeaderElectionRequestOrBuilder() {
        this.builder = TriggerLeaderElectionRequestProto.newBuilder();
    }

    public TriggerLeaderElectionRequestOrBuilder(TriggerLeaderElectionRequestProto request) {
        this.request = request;
        this.sender = RaftNodeEndpoint.wrap(request.getSender());
    }

    public TriggerLeaderElectionRequestProto getRequest() {
        return request;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(RaftNodeEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder setLastLogTerm(int lastLogTerm) {
        builder.setLastLogTerm(lastLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder setLastLogIndex(long lastLogIndex) {
        builder.setLastLogIndex(lastLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setTriggerLeaderElectionRequest(request);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "TriggerLeaderElectionRequest{builder=" + builder + "}";
        }

        return "TriggerLeaderElectionRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", "
                + "term=" + getTerm() + ", lastLogTerm=" + getLastLogTerm() + ", lastLogIndex=" + getLastLogIndex()
                + '}';
    }

    @Override
    public int getLastLogTerm() {
        return request.getLastLogTerm();
    }

    @Override
    public long getLastLogIndex() {
        return request.getLastLogIndex();
    }

    @Override
    public Object getGroupId() {
        return request.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return request.getTerm();
    }

}
