package team.dsys.dssearch.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.search.Doc;
import team.dsys.dssearch.search.IndexService;
import team.dsys.dssearch.cluster.shard.ShardService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
public class SearchService {

    @Autowired
    ShardService shardService;

    @Autowired
    IndexService indexService;

    public List<Doc> search(String query) {
        // 暂时跳过根据关联度获取docid的步骤，直接new docIds
        ArrayList<Integer> docIds = new ArrayList<Integer>();

        List<Doc> docs = fetchDocs(docIds);

        return docs;
    }

    // Sorted docIds
    private List<Doc> fetchDocs(List<Integer> docIds) {
        HashMap<Integer, List<Integer>> shard = shardService.shard(docIds);

        List<Doc> docs = indexService.batchGetDocs(shard);

        return docs;
    }

}
