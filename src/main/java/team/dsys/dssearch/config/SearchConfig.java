package team.dsys.dssearch.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Getter
@Configuration
@Component
public class SearchConfig {

    // nodeId
    @Value("${search.nodeId}")
    private int nid;

    @Value("${lucene.indexLibrary}")
    private String indexLibrary;

}