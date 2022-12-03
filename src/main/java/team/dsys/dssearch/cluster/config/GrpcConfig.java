package team.dsys.dssearch.cluster.config;

import com.typesafe.config.Config;
import team.dsys.dssearch.cluster.exception.ClusterServerException;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class GrpcConfig {

    public static final int DEFAULT_GRPC_TIMEOUT_SECONDS = 10;
    //TODo: set try limit for handling server failure?
    public static final int DEFAULT_RETRY_LIMIT = 20;

    private long grpcTimeoutSecs = DEFAULT_GRPC_TIMEOUT_SECONDS;

    private GrpcConfig() {
    }

    public static GrpcConfig from(@Nonnull Config config) {
        requireNonNull(config);
        try {
            GrpcConfigBuilder builder = new GrpcConfigBuilder();

            if (config.hasPath("grpc-timeout-secs")) {
                builder.setGrpcTimeoutSecs(config.getInt("grpc-timeout-secs"));
            }

            return builder.build();
        } catch (Exception e) {
            throw new ClusterServerException("Invalid configuration: " + config, e);
        }
    }

    public static GrpcConfigBuilder newBuilder() {
        return new GrpcConfigBuilder();
    }

    public long getGrpcTimeoutSecs() {
        return grpcTimeoutSecs;
    }

    @Override
    public String toString() {
        return "RpcConfig{" + "rpcTimeoutSecs=" + grpcTimeoutSecs + '}';
    }

    public static class GrpcConfigBuilder {

        private GrpcConfig config = new GrpcConfig();

        private GrpcConfigBuilder() {
        }

        public GrpcConfigBuilder setGrpcTimeoutSecs(long grpcTimeoutSecs) {
            if (grpcTimeoutSecs < 1) {
                throw new IllegalArgumentException(
                        "Rpc timeout seconds: " + grpcTimeoutSecs + " cannot be non-positive!");
            }

            config.grpcTimeoutSecs = grpcTimeoutSecs;
            return this;
        }

        public GrpcConfig build() {
            GrpcConfig builtConfig = this.config;
            this.config = null;
            return builtConfig;
        }

    }

}