package team.dsys.dssearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import team.dsys.dssearch.model.Doc;
import team.dsys.dssearch.service.SearchService;
import team.dsys.dssearch.vo.*;

import java.util.List;

/**
 * clients send requests via HTTP
 */

@Slf4j
@RestController("/search")
public class SearchController {

    @Autowired
    SearchService searchService;

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    SearchResponse search(String query) {
        if (query == null) {
            return new SearchResponse(-1, "Please enter your query!", null);
        }

        log.info("Start searching：" + query);
        List<Doc> searchResult = null;
        try {
            searchResult = searchService.search(query);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new SearchResponse(1, "ok", searchResult);
    }

    @PutMapping("/store")
    SearchResponse store(List<Doc> docs) {
        searchService.store(docs);
        return new SearchResponse(1, "ok", null);
    }
}
