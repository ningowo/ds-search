package team.dsys.dssearch.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Getter
@Configuration
public class SearchConfig {

    @Value("${search.host}")
    String host;

    @Value("${search.port}")
    int port;

    // nodeId
    @Value("${search.nodeId}")
    private int nid;

    @Value("${lucene.index}")
    private int index;

    @Value("${lucene.indexLibrary}")
    private String indexLibrary;

}
