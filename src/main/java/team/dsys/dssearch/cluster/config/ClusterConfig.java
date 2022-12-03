package team.dsys.dssearch.cluster.config;

import com.typesafe.config.Config;
import team.dsys.dssearch.cluster.exception.ClusterServerException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Define the config for nodes in the cluster
 */
public class ClusterConfig {

    private String id;
    private List<NodeEndpointConfig> initialEndpoints = Collections.emptyList();
    private String joinTo;

    private ClusterConfig() {
    }

    @Nonnull
    public static ClusterConfig from(@Nonnull Config config) {
        requireNonNull(config);

        try {
            ClusterConfigBuilder builder = newBuilder();

            if (config.hasPath("id")) {
                builder.setId(config.getString("id"));
            }

            if (config.hasPath("initial-endpoints")) {
                List<NodeEndpointConfig> initialEndpoints = config.getList("initial-endpoints").stream()
                        .map(configVal -> configVal.atKey("endpoint").getConfig("endpoint"))
                        .map(NodeEndpointConfig::from).collect(toList());
                builder.setInitialEndpoints(initialEndpoints);
            }

            if (config.hasPath("join-to")) {
                builder.setJoinTo(config.getString("join-to"));
            }

            return builder.build();
        } catch (Exception e) {
            throw new ClusterServerException("Invalid configuration: " + config, e);
        }
    }

    @Nonnull
    public static ClusterConfigBuilder newBuilder() {
        return new ClusterConfigBuilder();
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nullable
    public List<NodeEndpointConfig> getInitialEndpoints() {
        return initialEndpoints;
    }

    @Nullable
    public String getJoinTo() {
        return joinTo;
    }

    @Override
    public String toString() {
        return "ClusterConfig info {" + "id='" + id + '\'' + ", initialEndpoints=" + initialEndpoints + ", joinTo='"
                + joinTo + '\'' + '}';
    }

    public static class ClusterConfigBuilder {

        private ClusterConfig config = new ClusterConfig();

        private ClusterConfigBuilder() {
        }

        @Nonnull
        public ClusterConfigBuilder setId(@Nonnull String id) {
            config.id = requireNonNull(id);
            return this;
        }

        @Nonnull
        public ClusterConfigBuilder setInitialEndpoints(@Nonnull List<NodeEndpointConfig> endpointConfigs) {
            requireNonNull(endpointConfigs);
            config.initialEndpoints = unmodifiableList(new ArrayList<>(endpointConfigs));
            return this;
        }

        @Nonnull
        public ClusterConfigBuilder setJoinTo(@Nonnull String joinTo) {
            config.joinTo = requireNonNull(joinTo);
            return this;
        }

        @Nonnull
        public ClusterConfig build() {
            if (config == null) {
                throw new ClusterServerException("ClusterConfig is empty!");
            }

            if (config.id == null) {
                throw new ClusterServerException("Cannot build ClusterConfig without id!");
            }

            if ((config.initialEndpoints.isEmpty() && config.joinTo == null)
                    || (!config.initialEndpoints.isEmpty() && config.joinTo != null)) {
                throw new ClusterServerException("Either initial-endpoints or join-to must be set!");
            }

            if (config.joinTo == null && config.initialEndpoints.size() < 2) {
                throw new IllegalArgumentException(
                        "Could not bootstrap new cluster with " + config.initialEndpoints.size() + " endpoint!");
            }

            ClusterConfig builtConfig = this.config;
            this.config = null;
            return builtConfig;
        }

    }

}