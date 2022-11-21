package team.dsys.dssearch.util;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import team.dsys.dssearch.rpc.ShardService;
import team.dsys.dssearch.service.DocService;

public class TransportUtil {

    public static DocService.Connection buildShardConn(int nid, String host, int port, int timeout) throws TTransportException {
        TTransport transport = new TSocket(host, port, timeout);
        TProtocol protocol = new TBinaryProtocol(transport);
        ShardService.Client client = new ShardService.Client(protocol);
        transport.open();

        return new DocService.Connection(nid, host, port, client);
    }

}
