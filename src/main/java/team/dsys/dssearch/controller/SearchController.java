package team.dsys.dssearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import team.dsys.dssearch.service.SearchService;
import team.dsys.dssearch.vo.CommonResponse;
import team.dsys.dssearch.vo.DocVO;
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
    CommonResponse<List<DocVO>> search(String query, int size) {
        if (query == null) {
            return CommonResponse.createFailResult("Please enter your query!");
        }

        List<DocVO> searchResult = searchService.search(query, size);

        return CommonResponse.createSuccessResult(searchResult);
    }

    @PutMapping("/store")
    CommonResponse store(@RequestBody String content) {
        if (searchService.storeOne(content)) {
            return CommonResponse.createSuccessResult();
        } else {
            return CommonResponse.createFailResult("Failed to store.");
        }
    }

    @PutMapping("/store/list")
    CommonResponse storeList(@RequestBody List<String> contents) {
        if (searchService.store(contents)) {
            return CommonResponse.createSuccessResult();
        } else {
            return CommonResponse.createFailResult("Failed to store.");
        }
    }

}
