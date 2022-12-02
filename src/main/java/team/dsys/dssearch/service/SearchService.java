package team.dsys.dssearch.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.common.SnowflakeIDGenerator;
import team.dsys.dssearch.model.Doc;
import team.dsys.dssearch.shard.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
public class SearchService {

    @Autowired
    ShardServiceImpl shardServiceImpl;

    @Autowired
    DocService docService;

    public List<Doc> search(String query) {
        // 暂时跳过根据关联度获取docid的步骤，直接new docIds
        ArrayList<Long> docIds = new ArrayList<>();

        List<Doc> docs = fetchDocs(docIds);

        return docs;
    }

    // first send docs to primary shards' nodes, and the primary shard will replicate logs to nodes that replicas exist.
    public boolean store(List<Doc> docs) {
        // generate id
        SnowflakeIDGenerator generator = new SnowflakeIDGenerator();

        for (Doc doc : docs) {
            doc.set_id(generator.generate());
        }

        // primary shard's node id
        //Map node to docs eg node 1 (doc 1, doc 2, doc3..)
        HashMap<Integer, List<Doc>> nodeToDocs = shardServiceImpl.shardDocs(docs);
        boolean store = docService.store(nodeToDocs);

        return false;
    }

    // Sorted docIds
    private List<Doc> fetchDocs(List<Long> docIds) {
        HashMap<Integer, List<Long>> nodeToDocIds = shardServiceImpl.shardDocIds(docIds);

        List<Doc> docs = docService.batchGetDocs(nodeToDocIds);

        return docs;
    }

}
