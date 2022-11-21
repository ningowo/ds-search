package team.dsys.dssearch.cluster.config;

public class GrpcConfig {

    public static final int DEFAULT_GRPC_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_RETRY_LIMIT = 20;

    private long rpcTimeoutSecs = DEFAULT_GRPC_TIMEOUT_SECONDS;
}
