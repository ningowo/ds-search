package team.dsys.dssearch.server;

import cluster.external.shard.proto.ShardInfo;
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
import team.dsys.dssearch.ClusterServiceManagerImpl;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.service.ShardServiceImpl;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class ThriftServer {

    @Value("${search.server.port}")
    private int searchPort;

    @Autowired
    ShardServiceImpl shardService;

    @Autowired
    ClusterServiceManagerImpl clusterService;

    public void start() {
        // register data node info to cluster
        // test
        List<ShardInfo> shardInfoList = new ArrayList<>();
        if (searchPort == 6000) {
            shardInfoList.add(ShardInfo.newBuilder().setShardId(1).setIsPrimary(true).build());
            shardInfoList.add(ShardInfo.newBuilder().setShardId(2).setIsPrimary(false).build());
        } else if (searchPort == 6001){
            shardInfoList.add(ShardInfo.newBuilder().setShardId(2).setIsPrimary(true).build());
            shardInfoList.add(ShardInfo.newBuilder().setShardId(1).setIsPrimary(false).build());
        }

        boolean connectToCluster = clusterService.sendShardInfoToCluster(shardInfoList);
        if (connectToCluster) {
            log.info("Connected to cluster");
        } else {
            log.info("Failed to connected to cluster");
        }

        try {
            TServerTransport transport = new TServerSocket(searchPort);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            args.protocolFactory(protocolFactory);

            TProcessor processor = new ShardService.Processor<ShardService.Iface>(shardService);
            args.processor(processor);

            TServer server = new TThreadPoolServer(args);
            log.info("Thrift server start with port {}", searchPort);
            server.serve();
        } catch (Exception e) {
            log.error("Failed to start thrift server! port={}", searchPort);
        }

    }
}