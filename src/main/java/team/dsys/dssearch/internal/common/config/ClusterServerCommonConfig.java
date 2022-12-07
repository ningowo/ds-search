package team.dsys.dssearch.internal.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.readAllLines;
import static java.util.stream.Collectors.toList;

public class ClusterServerCommonConfig {
    private String clusterId;
    private Map<String, String> clusterServerAddress = new LinkedHashMap<>();

    private ClusterServerCommonConfig(String clusterId, Map<String, String> clusterServerAddress) {
        this.clusterId = clusterId;
        this.clusterServerAddress = clusterServerAddress;

    }

    public static ClusterServerCommonConfig getClusterCommonConfig(String configFilePath) {
        String configFile = readConfig(configFilePath);
        if (configFile != null) {
            return parseConfig(configFile);
        } else {
            throw new IllegalArgumentException("Config File is null!");
        }

    }

    public String getClusterId() {
        return clusterId;
    }

    public Map<String, String> getClusterServerAddress() {
        return clusterServerAddress;
    }

    private static String readConfig(String configFilePath) {
        //String configPath = "/src/main/java/team/dsys/dssearch/internal/common/config/cluster.conf";
        //String prop = System.getProperty("user.dir") + configPath;
        try {
            String configString = String.join("\n", readAllLines(Paths.get(configFilePath), StandardCharsets.UTF_8));
            return configString;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot read config file from " + configFilePath);
            return null;
        }

    }

    private static ClusterServerCommonConfig parseConfig(String configFile) {
        Config config = ConfigFactory.parseString(configFile);
        String id = new String();
        Map<String, String> map = new LinkedHashMap<>();
        try {
            if (config.hasPath("cluster")) {
                Config clusterConfig = config.getConfig("cluster");

                if (clusterConfig.hasPath("cluster-id")) {
                    id = clusterConfig.getString("cluster-id");
                }

                if (clusterConfig.hasPath("initial-endpoints")) {
                    List<Config> endPointsConfigs = clusterConfig.getList("initial-endpoints").stream()
                            .map(configValue -> configValue.atKey("endpoint").getConfig("endpoint")).collect(toList());

                    for (Config cc : endPointsConfigs) {
                        String clusterServerId = cc.getString("id");
                        String address = cc.getString("address");

                        map.put(clusterServerId, address);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (id == null || map == null) {
            throw new IllegalArgumentException("Missing clusterId in config file");
        }

        return new ClusterServerCommonConfig(id, map);

    }


}
