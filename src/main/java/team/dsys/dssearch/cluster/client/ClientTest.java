package team.dsys.dssearch.cluster.client;

import cluster.proto.*;
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
public class ClientTest {
    private final ShardRequestHandlerGrpc.ShardRequestHandlerBlockingStub blockingStub;

    private ShardResponse response;

    public ClientTest(Channel channel) {
        blockingStub =  ShardRequestHandlerGrpc.newBlockingStub(channel);

    }

    public void sendPut() {

        try {
            response = blockingStub.put(PutRequest.newBuilder().setKey("apple").setVal(Val.newBuilder().setNum(12).build()).build());

        } catch (StatusRuntimeException e) {
            System.out.println("Error"+e);
            return;
        }
        System.out.println("Response" + response);
    }

    public void sendGet() {

        try {
            response = blockingStub.get(GetRequest.newBuilder().setKey("apple").build());

        } catch (StatusRuntimeException e) {
            System.out.println("Error"+e);
            return;
        }
        System.out.println("Response" + response);
    }

    public static void main(String[] args) throws Exception {
        String target = "localhost:6701";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();

        try {
            ClientTest client = new ClientTest(channel);

            client.sendPut();
            client.sendGet();
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }


}
