namespace java team.dsys.dssearch.rpc

service ShardService {

    CommonResponse commonReq(1: CommonRequest req),

    GetResponse get(1: i32 docId),

    GetResponse batchGet(1: list<string> docIds),

    CommonResponse store(1: list<Doc> docs),

    bool prepare(1: Transaction trans),

    bool commit(1: Transaction trans),

    bool remove(1: Transaction trans),

    list<ScoreAndDocId> queryTopN(1: string query, 2: i32 n; 3: i32 shardId),

    list<Doc> getDocList(1: list<i32> sortedDocIds, 2: i32 shardId)

}

struct ScoreAndDocId {
    1: double score,
    2: i32 docId
}

struct CommonRequest {
    1: i32 id,
    2: i8 type,
    3: string content
}

struct GetResponse {
    1: bool success,
    2: i32 docId,
    3: Doc doc
}

struct CommonResponse {
    1: bool success,
    2: string msg
}

struct Doc {
    1: i32 index,
    2: i32 id,
    3: string content
}
struct Transaction {
    1: i32 transId,
    2: i32 key,
    3: Doc val
}
