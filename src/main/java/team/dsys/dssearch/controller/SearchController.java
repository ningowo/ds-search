package team.dsys.dssearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.service.SearchService;
import team.dsys.dssearch.vo.SearchResponse;

import java.util.List;

/**
 * clients send requests via HTTP
 * test: http://localhost:8081/search?query=a
 */

@Slf4j
@RestController
@RequestMapping("/s")
public class SearchController {

    @Autowired
    SearchService searchService;

    @GetMapping("/search")
    SearchResponse search(String query) {
        if (query == null) {
            return new SearchResponse(-1, "Please enter your query!", null);
        }

        List<Doc> searchResult = searchService.search(query, 1);

        return new SearchResponse(1, "ok", searchResult);
    }

    @PutMapping("/store")
    SearchResponse store(@RequestBody List<Doc> docs) throws TException {

        searchService.store(docs);
        return new SearchResponse(1, "ok", null);
    }
}
