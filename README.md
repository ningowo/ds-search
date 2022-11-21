# ds-search

两条逻辑

1. 用户CRUD -> service -> shard定位doc -> 获取doc

2. 心跳包发现集群节点变动 -> 发起选举 -> 更新配置

ClusterService：维护集群信息
DocService：负责doc的存取
ShardService：负责提供远程服务
