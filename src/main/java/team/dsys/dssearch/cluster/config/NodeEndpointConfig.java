package team.dsys.dssearch.cluster.config;

import com.typesafe.config.Config;
import team.dsys.dssearch.cluster.exception.ClusterServerException;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;

import static java.util.Objects.requireNonNull;

public class NodeEndpointConfig {

    private String id;
    private String address;

    private NodeEndpointConfig() {
    }

    public static NodeEndpointConfig from(@Nonnull Config config) {
        requireNonNull(config);
        try {
            return new NodeEndpointConfigBuilder().setId(config.getString("id"))
                    .setAddress(config.getString("address")).build();
        } catch (Exception e) {
            throw new ClusterServerException("Invalid configuration: " + config, e);
        }
    }

    public static NodeEndpointConfigBuilder newBuilder() {
        return new NodeEndpointConfigBuilder();
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nonnull
    public String getAddress() {
        return address;
    }

    @Nonnull
    public InetSocketAddress getSocketAddress() {
        String[] addressTokens = address.split(":");
        return new InetSocketAddress(addressTokens[0], Integer.parseInt(addressTokens[1]));
    }

    @Override
    public String toString() {
        return "NodeEndpointConfig info {" + "id='" + id + '\'' + ", address='" + address + '\'' + '}';
    }

    public static class NodeEndpointConfigBuilder {
        private NodeEndpointConfig config = new NodeEndpointConfig();

        private NodeEndpointConfigBuilder() {
        }

        @Nonnull
        public NodeEndpointConfigBuilder setId(@Nonnull String id) {
            config.id = requireNonNull(id);
            return this;
        }

        @Nonnull
        public NodeEndpointConfigBuilder setAddress(@Nonnull String address) {
            config.address = requireNonNull(address);
            return this;
        }

        @Nonnull
        public NodeEndpointConfig build() {
            if (config == null) {
                throw new ClusterServerException("NodeEndpointConfig is empty!");
            }

            if (config.id == null) {
                throw new ClusterServerException("Cannot build NodeEndpointConfig without unique id!");
            }

            if (config.address == null) {
                throw new ClusterServerException("Cannot build NodeEndpointConfig without address!");
            }

            NodeEndpointConfig builtConfig = this.config;
            this.config = null;
            return builtConfig;
        }

    }

}
