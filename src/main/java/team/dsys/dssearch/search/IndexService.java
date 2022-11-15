package team.dsys.dssearch.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexService {

    public Doc getDoc(Integer node, Integer docId) {
        return null;
    }

    // multi-thread can be used later
    public List<Doc> batchGetDocs(HashMap<Integer, List<Integer>> nodesAndDocIds) {
        for (Map.Entry<Integer, List<Integer>> entry : nodesAndDocIds.entrySet()) {
            // using stub to get docs
        }
        return null;
    }

}
