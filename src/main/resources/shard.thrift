namespace java team.dsys.dssearch.rpc

service ShardService {

    CommonRpcResponse store(1: i32 shardId, 2: list<Doc> docs),

    CommonRpcResponse transferStoreReq(1: i32 shardId, 2: list<Doc> docs),

    list<ScoreAndDocId> queryTopN(1: string query, 2: i32 n; 3: i32 shardId),

    list<Doc> getDocList(1: list<i32> sortedDocIds, 2: i32 shardId)

}

struct ScoreAndDocId {
    1: double score,
    2: i32 docId
}

struct Doc {
    1: i32 index,
    2: i32 id,
    3: string content
}

struct CommonRpcResponse {
    1: bool success,
    2: string msg
}