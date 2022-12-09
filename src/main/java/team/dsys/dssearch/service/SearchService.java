package team.dsys.dssearch.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.util.SnowflakeIDGenerator;
import team.dsys.dssearch.vo.DocVO;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SearchService {

    @Autowired
    ShardServiceImpl shardServiceHandler;

    @Autowired
    DocService docService;

    @Autowired
    StoreEngine storeEngine;

    @Autowired
    SearchConfig searchConfig;

    SnowflakeIDGenerator generator = new SnowflakeIDGenerator();

    public List<Doc> search(String query) {
        // 获取docid并排序
        // for all shards
//        List<Shard> nodeList;
//        for (Node node : nodeList) {
//
//            List<ScoreDoc> resultDocIds = storeEngine.queryTopN(query, 2, 1);
//            log.info("Get total {} docs by search all nodes", resultDocIds.size());
//            Pair<Integer, ScoreDoc> pair = new Pair<>(shardId, ScoreDoc);
//        }
//
//
//        List<ScoreDoc> collect = resultDocIds.stream().sorted((o1, o2) -> (int) (o1.score - o2.score)).collect(Collectors.toList());
//        for (ScoreDoc scoreDoc : collect) {
//            System.out.println(scoreDoc);
//        }
//
//        // score doc
//        List<Integer> topList = collect.stream().map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList());
//        List<Doc> topDocList = storeEngine.getDocList(topList, shardId);
//        log.info("");
//        log.info("Get docs by id:");
//        for (Doc doc : topDocList) {
//            System.out.println(doc);
//        }

        // remove Lucene dirs
        File dir = new File("./lucene/");
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                for (File listFile : file.listFiles()) {
                    listFile.delete();
                }
            }
        }
        log.info("Shard files deleted");

//        return topDocList;

        return null;
    }

    // first send docs to primary shards' nodes, and the primary shard will replicate logs to nodes that replicas exist.
    public boolean store(List<DocVO> docVOLists) throws TException {
        List<Doc> docs = docVOLists.stream().
                map(docVO -> new Doc(docVO.index, docVO.id, docVO.content)).collect(Collectors.toList());

        // generate global unique doc id
        for (Doc doc : docs) {
            doc.set_id(generator.generate());
        }

        // 获取集群的状态
        // primary shard's node id
        //Map node to docs eg node 1 (doc 1, doc 2, doc3..)
        // [key = node id, value = [key = shardId, value=List<Doc>>]]
        HashMap<Integer, HashMap<Integer, List<Doc>>> nodeToDocs;

//        // 1. 存本机
//        // 如果当前节点上有doclist所需的主分片，直接存了
//        List<Doc> docListOnCurrentNode = nodeToDocs.get(searchConfig.getNid());
//        if (docListOnCurrentNode != null) {
//            storeEngine.writeDocList(docListOnCurrentNode);
//        }
//
//        // todo
//        // 2. 存远程
//        // 原来是这个 boolean store = docService.store(nodeToDocs);
//        // 转发nodeToDocs到主分片所在节点(RPC调用)
//        Shards shardsOnCurrentNode = ClusterServiceImpl.getShardsOnCurrentNode();
//        boolean hasPrimary = false;
//        for (Shard shard: shardsOnCurrentNode.idToShards.values()) {
//            if (shard.isPrimary()) {
//                shardServiceHandler.store(nodeToDocs);
//            }
//        }

        return false;
    }
//
//    // Sorted docIds
//    private List<Doc> fetchDocs(List<Long> docIds) {
//        HashMap<Integer, List<Long>> nodeToDocIds = shardServiceImpl.shardDocIds(docIds);
//
//        List<Doc> docs = docService.batchGetDocs(nodeToDocIds);
//
//        return docs;
//    }

}
