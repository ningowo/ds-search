package team.dsys.dssearch.cluster;

import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllLines;
import static java.util.Objects.requireNonNull;

public class RunCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunCluster.class);

    public static void main(String[] args) {

        //reading config file
        //model config file reference: resources/node1.conf; args: config file absolute path
        String configFileName = "";
        if (args.length > 0) {
            configFileName = args[0];
        } else {
            LOGGER.error("Config file name is invalid.");
            System.exit(-1);
        }

        LOGGER.info("Reading config from " + configFileName);

        ClusterService cluster;
        ClusterServiceConfig config = readConfigFile(configFileName);
        //bootstrap a server or join to current cluster using current config
        if (config.getClusterConfig().getJoinTo() == null) {
            cluster = new ServerBootstrapper(requireNonNull(config)).get();

        } else {
            cluster = new ServerJoiner(requireNonNull(config), true).get();
        }

        cluster.awaitTermination();
    }


    private static ClusterServiceConfig readConfigFile(String configFileName) {
        try {
            //build ClusterServiceConfig from config file
            String config = String.join("\n", readAllLines(Paths.get(configFileName), StandardCharsets.UTF_8));
            return ClusterServiceConfig.from(ConfigFactory.parseString(config));
        } catch (IOException e) {
            LOGGER.error("Cannot read config file: " + configFileName, e);
            System.exit(-1);
            return null;
        }
    }

}

