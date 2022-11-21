package team.dsys.dssearch.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan
@PropertySource("classpath:cluster.properties")
public class ClusterConfig {

    @Value("${cluster.main-shard-num}")
    private int mainShardNum;

}
