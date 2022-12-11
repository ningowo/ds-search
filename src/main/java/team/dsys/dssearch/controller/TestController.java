package team.dsys.dssearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.ScoreDoc;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.service.SearchService;
import team.dsys.dssearch.vo.DocVO;
import team.dsys.dssearch.vo.SearchResponse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchService searchService;

    @GetMapping("/eng/store/simple")
    public void testEngineStoreSimple() {
        List<String> docsContentList = new ArrayList<>();
        docsContentList.add("there lalala 1");
        docsContentList.add("test here, let's test");
        docsContentList.add("Test here 11 222");
        docsContentList.add("Test 1 2");

        boolean b2 = storeEngine.writeDocList(docsContentList, 2);
        log.info("write doc to shard {}: {}", 2, b2);
    }

    @GetMapping("/eng/store")
    public void testEngineStore() {
        List<Doc> docsInShard1 = new ArrayList<>();

        docsInShard1.add(new Doc(1, 1, "there lalala 1"));
        docsInShard1.add(new Doc(1, 3, "test here, let's test"));

        boolean b1 = storeEngine.writeDocList(docsInShard1, 1);
        log.info("write doc to shard {}: {}", 1, b1);

        List<Doc> docsInShard2 = new ArrayList<>();
        docsInShard2.add(new Doc(1, 2, "Test here 11 222"));
        docsInShard2.add(new Doc(1, 4, "Test 1 2"));

        boolean b2 = storeEngine.writeDocList(docsInShard2, 2);
        log.info("write doc to shard {}: {}", 2, b2);
    }

    @GetMapping("/eng/query")
    public void testEngineStoreQuery(String query, int shardId) {
        List<ScoreDoc> b2 = storeEngine.queryTopN(query, 1, 2);
        log.info("Got docs:");
        for (ScoreDoc scoreDoc : b2) {
            log.info("{}", scoreDoc.toString());
        }
    }

    @GetMapping("/engine")
    public void test(String query, int shardId) {
        List<Doc> docsInShard1 = new ArrayList<>();
        docsInShard1.add(new Doc(1, 1, "there lalala 1"));
        docsInShard1.add(new Doc(1, 3, "test here, let's test"));

        boolean b1 = storeEngine.writeDocList(docsInShard1, 1);
        log.info("write doc to shard {}: {}", 1, b1);

        List<Doc> docsInShard2 = new ArrayList<>();
        docsInShard2.add(new Doc(1, 2, "Test here 11 222"));
        docsInShard2.add(new Doc(1, 4, "Test 1 2"));

        boolean b2 = storeEngine.writeDocList(docsInShard2, 2);
        log.info("write doc to shard {}: {}", 2, b2);

        // 获取docid并排序
        List<ScoreDoc> resultDocIds = storeEngine.queryTopN(query, 2, 1);
        log.info("");
        log.info("Get {} docs by search", resultDocIds.size());
        List<ScoreDoc> collect = resultDocIds.stream().sorted((o1, o2) -> (int) (o1.score - o2.score)).collect(Collectors.toList());
        for (ScoreDoc scoreDoc : collect) {
            System.out.println(scoreDoc);
        }

        // score doc
        List<Integer> topList = collect.stream().map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
        List<Doc> topDocList = storeEngine.getDocList(topList, shardId);
        log.info("");
        log.info("Get docs by id:");
        for (Doc doc : topDocList) {
            System.out.println(doc);
        }

        // remove Lucene dirs
        File dir = new File("./lucene/");
        for (File file : dir.listFiles()) {
            file.delete();
        }
        log.info("Shard files deleted");
    }

    // localhost:8080/test/store
    @GetMapping("/store")
    SearchResponse store() throws TException {
        List<Doc> docs = new ArrayList<>();
        docs.add(new DocVO(1, 1, "there lalala 1"));
        docs.add(new DocVO(1, 2, "Test here 11 222"));
        docs.add(new DocVO(1, 3, "test here, let's test"));
        docs.add(new DocVO(1, 4, "Test 1 2"));

        searchService.store(docs);
        return new SearchResponse(1, "ok", null);
    }
}
