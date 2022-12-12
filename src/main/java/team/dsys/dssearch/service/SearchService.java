package team.dsys.dssearch.service;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import team.dsys.dssearch.cluster.ClusterServiceImpl;
import team.dsys.dssearch.common.SnowflakeIDGenerator;
import team.dsys.dssearch.internal.common.ClusterServiceManager;
import team.dsys.dssearch.internal.common.impl.ClusterServiceManagerImpl;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.shard.*;
import team.dsys.dssearch.vo.DocVO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Service
public class SearchService {

    @Autowired
    ShardServiceImpl shardServiceImpl;

    @Autowired
    DocService docService;

    @Value("${cluster.main-shard-num}")
    private int mainShardNum;

    @Value("${search.nodeId}")
    private int nodeId;

    private final String CONFIG_FILE_PATH = "../../../resources/cluster.conf";
    private final ClusterServiceManager clusterServiceManager = new ClusterServiceManagerImpl(nodeId, CONFIG_FILE_PATH);

    SnowflakeIDGenerator generator = new SnowflakeIDGenerator();

    public SearchService() throws TimeoutException {
    }

    public List<Doc> search(String query) {
        // 暂时跳过根据关联度获取docid的步骤，直接new docIds
        ArrayList<Long> docIds = new ArrayList<>();

        List<Doc> docs = fetchDocs(docIds);

        return docs;
    }

    // first send docs to primary shards' nodes, and the primary shard will replicate logs to nodes that replicas exist.
    public boolean store(List<DocVO> docVOLists) throws TException {
        List<Doc> docs = docVOLists.stream().
                map(docVO -> new Doc(docVO.index, docVO.id, docVO.content)).collect(Collectors.toList());

        for (Doc doc : docs) {
            doc.set_id(generator.generate());
        }

        // 获取集群的状态
        // primary shard's node id
        //Map node to docs eg node 1 (doc 1, doc 2, doc3..)
        // [key = node id, value = [key = shardId, value=List<Doc>>]]
        HashMap<Integer, HashMap<Integer, List<Doc>>> nodeToDocs = new HashMap<>();

        for (Doc doc : docs) {
            int shardId = doc.get_id() % mainShardNum;
            ShardResponse shardResponse = clusterServiceManager.getShardInfo(GetShardRequest.newBuilder().addShardId(shardId));
            int primaryNodeId = nodeId;
            for (ShardInfoWithDataNodeInfo shardInfoWithDataNodeInfo : shardResponse.getShardInfoWithDataNodeInfo()) {
                if (shardInfoWithDataNodeInfo.getShardInfo().isPrimary()) {
                    primaryNodeId = shardInfoWithDataNodeInfo.getDataNodeInfo().getDataNodeId();
                    break;
                }
            }
            nodeToDocs.set(
                    primaryNodeId, nodeToDocs.getOrDefault(
                            primaryNodeId, new HashMap<>()).getOrDefault(
                                    shardId, new ArrayList<>()).add(doc));
        }

        // 1. 存本机
        // 如果当前节点上有doclist所需的主分片，直接存了
        HashMap<Integer, List<Doc>> docListOnCurrentNode = nodeToDocs.get(nodeId);
        if (docListOnCurrentNode != null) {
            storeEngine.writeDocList(docListOnCurrentNode);
        }

        // todo
        // 2. 存远程
        // 原来是这个 boolean store = docService.store(nodeToDocs);
        // 转发nodeToDocs到主分片所在节点(RPC调用)
        for (int node : docListOnCurrentNode.keySet()) {
            if (node == nodeId) {
                continue;
            }
            // how to send doc list to another node?
            callNode(node, docListOnCurrentNode.get(node));
        }
        return false;
    }

    // Sorted docIds
    private List<Doc> fetchDocs(List<Long> docIds) {
        HashMap<Integer, List<Long>> nodeToDocIds = shardServiceImpl.shardDocIds(docIds);

        List<Doc> docs = docService.batchGetDocs(nodeToDocIds);

        return docs;
    }

}
