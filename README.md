# ds-search

两条逻辑

1. 用户CRUD -> server -> service -> shard定位doc -> 获取doc

2. 心跳包发现集群节点变动 -> 发起选举 -> 更新配置