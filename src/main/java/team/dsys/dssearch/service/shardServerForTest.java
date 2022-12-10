package team.dsys.dssearch.service;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.shard.ShardServiceImpl;

import java.util.ArrayList;
import java.util.List;

public class shardServerForTest {

        public static ShardServiceImpl handler;

        public static ShardService.Processor processor;

        public static void main(String [] args) {
            if (args.length < 6) {
                System.out.println("Please input <Port1> <Port2> <Port3> <Port4> <Port5> <coordinatorPort>");
                System.exit(0);
            }

            List<Integer> ports = new ArrayList<>();
            for (int i = 0; i <= 5; i++) {
                ports.add(Integer.parseInt(args[i]));
            }
//            int port = Integer.parseInt(args[5]);
            for (int i = 0; i <= 5; i++) {
                int port = Integer.parseInt(args[i]);
                try {
                    handler = new ShardServiceImpl(ports);
                    processor = new ShardService.Processor(handler);

                    Runnable simple = new Runnable() {
                        public void run() {
                            simple(processor, port);
                        }
                    };
                    new Thread(simple).start();

                } catch (Exception x) {
                    x.printStackTrace();
                }
            }
        }

        public static void simple(ShardService.Processor processor, int port) {
            try {
                TServerTransport serverTransport = new TServerSocket(port);
                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                System.out.println("Starting the coordinator server...");
                server.serve();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
}
