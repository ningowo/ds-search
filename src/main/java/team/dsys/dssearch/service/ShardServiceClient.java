package team.dsys.dssearch.service;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import team.dsys.dssearch.rpc.CommonResponse;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.rpc.ShardService;

import java.util.ArrayList;
import java.util.List;

public class ShardServiceClient {
    public static void main(String [] args) {
        if (args.length < 6) {
            System.out.println("Please input <Port1> <Port2> <Port3> <Port4> <Port5> <coordinatorPort>");
            System.exit(0);
        }
        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ports.add(Integer.parseInt(args[i]));
        }
        try {
            List<TTransport> transports = new ArrayList<>();
            List<ShardService.Client> clients = new ArrayList<>();
            for (int port : ports) {
                TTransport transport;
                transport = new TSocket("localhost", port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ShardService.Client client = new ShardService.Client(protocol);
                transports.add(transport);
                clients.add(client);
            }
            perform(clients);
            for (TTransport transport : transports) {
                transport.close();
            }
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    /**
     * Five PUT, DELETE, and GET test cases, additional five testcases to test the DELETE whether succeed or not
     * The logical I used to design my testcases is that we randomly put key-value into the map, if our two process commit
     * worked well, it will ensure all five hashmap have the consistent value, we can just randomly get value from random map,
     * if we could get the value while we haven't put the value in this map before, which means the coordinator works well and sync the
     * value to all five maps
     * @param clients
     * @throws TException
     */
    private static void perform(List<ShardService.Client> clients) throws TException {
        List<Doc> docs1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Doc doc = new Doc();
            doc._index = "index" + String.valueOf(i);
            doc._id = i;
            doc.content = "content" + String.valueOf(i);
            docs1.add(doc);
        }
        List<Doc> docs2 = new ArrayList<>();

        for (int i = 5; i < 10; i++) {
            Doc doc = new Doc();
            doc._index = "index" + String.valueOf(i);
            doc._id = i;
            doc.content = "content" + String.valueOf(i);
            docs2.add(doc);
        }
        List<Doc> docs3 = new ArrayList<>();

        for (int i = 10; i < 15; i++) {
            Doc doc = new Doc();
            doc._index = "index" + String.valueOf(i);
            doc._id = i;
            doc.content = "content" + String.valueOf(i);
            docs3.add(doc);
        }
        CommonResponse store1 = clients.get(0).store(docs1);
        CommonResponse store2 = clients.get(1).store(docs2);
        CommonResponse store3 = clients.get(2).store(docs3);

        System.out.println(store1);
        System.out.println(store2);
        System.out.println(store3);

    }
}
