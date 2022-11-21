namespace java teams.dsys.dssearch.rpc

service ShardService {

    CommonResponse commonReq(1: CommonRequest req);

    GetResponse get(1: i32 docId);

    GetResponse batchGet(1: list<string> docIds)

}

struct CommonRequest {
    1: i32 id,
    2: i8 type,
    3: string content
}

struct GetResponse {
    1: bool success,
    2: i32 docId,
    3: string doc
}

struct CommonResponse {
    1: bool success,
    2: string msg
}
