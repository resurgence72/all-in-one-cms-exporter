### All-in-one Cloud CMS exporter
> 无状态服务，实现了如下
>
> - 阿里云 / ali
> - 腾讯云 / tc
> - AWS / aws (未实现)
> - Google / google
> - Megaport / megaport
>
> 云厂商自定义 namespace 的 metrics 拉取功能； 将数据基于 remote write 推送之后端 TSDB

#### 为什么要做 All-in-one Cloud CMS exporter
```text
因为市面上 cloud exporter 无法满足需求

开源cloud cms exporter 存在的问题
1. 采集周期调整需要修改配置,周期与exporter耦合;
2. 指标和region需要手动指定；多指标多region场景配置麻烦；
3. 有状态服务，无法高可用，单点问题；多副本重复采集；
4. 每个云厂商每个子账号都单独维护一个exporter,组件分散不好管理；
5. 默认产品指标仅携带instance_id，实例不做源数据关联，指标信息缺失，问题定位不清晰；
6. 基于配置文件显式声明 cloud skey sid，存在安全风险；
7. ...


All-in-one Cloud CMS exporter 解决了哪些问题
1. 调用周期 账密文件 产品ns 与 exporter 解耦，独立组件cycle（未开源）负责调度和配置分发；
2. exporter 无状态，多副本幂等，支持高可用及分片;
3. 多云厂商云 cms 接口统一接入，统一管理；
4. freamwork形式方便二次开发，interface形式自定义不同ns源数据处理逻辑；
5. pull 模型调整为 push， 改善巨量指标下 prometheus pull 拉取时长过长问题；
6. 实现 remote_write 协议，原生支持 prometheus stack; 实现 relabel 支持 remote_write 多TSDB;
```

#### 配置文件说明

> 配置文件默认在 cmd/watcher4metrics/watcher4metrics.yml  
> ```
> global:
>   auto_reload: false				# 动态监听配置文件变动
>
> report:
>   batch: 5000
>   remote_write:
>     - url: http://172.19.64.103:19000/prometheus/v1/write
>       remote_timeout: 10s
>       write_relabel_configs:
>         - source_labels: ["__name__"]
>           regex: "^net.+"
>           action: "keep"
>
> http:
>   listen: :8081
>   timeout: 35s						# 接口即成了 pprof, timeout 太低会导致 pprof timeout
> ```