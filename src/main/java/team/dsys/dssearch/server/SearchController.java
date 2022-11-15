package team.dsys.dssearch.server;

import org.elasticsearch.action.search.SearchResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * clients send requests via HTTP
 */

@RestController
public class SearchController {

    @GetMapping
    SearchResponse search(SearchRequest request) {
        return null;
    }

}
