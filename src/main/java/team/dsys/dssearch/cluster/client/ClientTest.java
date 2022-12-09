package team.dsys.dssearch.cluster.client;

import cluster.external.shard.proto.*;
import cluster.internal.management.proto.GetRaftNodeReportRequest;
import cluster.internal.management.proto.GetRaftNodeReportResponse;
import cluster.internal.management.proto.ManagementRequestHandlerGrpc;
import cluster.internal.management.proto.RaftNodeReportProto;
import cluster.internal.raft.proto.RaftEndpointProto;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

/**
 * A class for client test(sending put, get, remove request to server)
 * @ShardRequestHandlerGrpc is the grpc file auto generated for proto file @ShardRequest.proto
 *
 */
// connect
    //get
    //put
//public class ClientTest {
//    private final ShardRequestHandlerGrpc.ShardRequestHandlerBlockingStub blockingStub;
//
//    private ShardResponse response;
//
//    public ClientTest(Channel channel) {
//        blockingStub =  ShardRequestHandlerGrpc.newBlockingStub(channel);
//
//    }
//
//    public void sendPut() {
//
//        try {
//            response = blockingStub.put(PutRequest.newBuilder().setKey("apple").setVal(Val.newBuilder().setNum(12).build()).build());
//
//        } catch (StatusRuntimeException e) {
//            System.out.println("Error"+e);
//            return;
//        }
//        System.out.println("Response" + response);
//    }
//
//    public void sendGet() {
//
//        try {
//            response = blockingStub.get(GetRequest.newBuilder().setKey("apple").build());
//
//        } catch (StatusRuntimeException e) {
//            System.out.println("Error"+e);
//            return;
//        }
//        System.out.println("Response" + response);
//    }
//
//    public static void main(String[] args) throws Exception {
//        //1. connect to any node in the cluster and get leader address
//        GetRaftNodeReportResponse reportResponse = getReport("localhost:6701");
//        RaftNodeReportProto report = reportResponse.getReport();
//        RaftEndpointProto leaderEndpoint = report.getTerm().getLeaderEndpoint();
//        String leaderAddress = reportResponse.getEndpointAddressOrDefault(leaderEndpoint.getId(), null);
//
//        //2.connect to leader and send put/get request
//        ManagedChannel leaderChannel = ManagedChannelBuilder.forTarget(leaderAddress)
//                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
//                // needing certificates.
//                .usePlaintext()
//                .build();
//
//        try {
//            ClientTest client = new ClientTest(leaderChannel);
//
//            client.sendPut();
//            client.sendGet();
//        } finally {
//            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
//            // resources the channel should be shut down when it will no longer be used. If it may be used
//            // again leave it running.
//            leaderChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
//        }
//    }
//
//    private static GetRaftNodeReportResponse getReport(String address) {
//        ManagedChannel reportChannel = ManagedChannelBuilder.forTarget(address).usePlaintext().disableRetry().directExecutor().build();;
//        GetRaftNodeReportResponse reportResponse;
//        try {
//            reportResponse = ManagementRequestHandlerGrpc.newBlockingStub(reportChannel)
//                    .getRaftNodeReport(GetRaftNodeReportRequest.getDefaultInstance());
//        } finally {
//            reportChannel.shutdownNow();
//        }
//
//        return reportResponse;
//    }
//
//
//}
