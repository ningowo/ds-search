package team.dsys.dssearch.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.ScoreDoc;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import team.dsys.dssearch.ClusterServiceManagerImpl;
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.model.Shards;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.service.SearchService;
import team.dsys.dssearch.vo.SearchResponse;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchService searchService;

    @Autowired
    ClusterServiceManagerImpl serviceManager;

    @Autowired
    SearchConfig searchConfig;

    @GetMapping("/t")
    public void test() {
        HashMap<Integer, Shards> info = serviceManager.getTotalNodeToShardsMap();
        System.out.println(info);

//        ArrayList<Integer> list = new ArrayList<>();
//        list.add(1);
//        System.out.println(serviceManager.getNodeOfPrimaryShard(list));
    }

    @GetMapping("/clear")
    public String clear(int shardId) {
        String rootPath = String.format("%s/", searchConfig.getIndexLibrary());

        if (shardId == -1) {
            del(new File(rootPath));
        }

        return "ok";
    }

    private void del(File f) {
        if (f.exists()) {
            for (File file : f.listFiles()) {
                if (file.isDirectory()) {
                    del(file);
                }else {
                    file.delete();
                }
            }
            f.delete();
        }
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

}
