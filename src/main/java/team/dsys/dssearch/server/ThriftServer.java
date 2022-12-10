package team.dsys.dssearch.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.service.ShardServiceImpl;

@Slf4j
@Component
public class ThriftServer {

    @Value("${search.port}")
    private int port;

    @Autowired
    ShardServiceImpl shardService;

    public void start() {
        try {
            TServerTransport transport = new TServerSocket(port);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            args.protocolFactory(protocolFactory);

            TProcessor processor = new ShardService.Processor<ShardService.Iface>(shardService);
            args.processor(processor);

            TServer server = new TThreadPoolServer(args);
            log.info("Thrift server start with port {}", port);
            server.serve();
        } catch (Exception e) {
            log.error("Failed to start thrift server! port={}", port);
        }

    }
}