package team.dsys.dssearch.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class SearchConfig {

    @Value("${search.server.host}")
    String host;

    @Value("${search.server.port}")
    int port;

    @Value("${search.server.nodeId}")
    int nid;

    @Value("${search.engine.index}")
    int index;

    @Value("${search.engine.primaryShardNum}")
    int primaryShardNum;

    @Value("${search.engine.indexLibrary}")
    String indexLibrary;

}
