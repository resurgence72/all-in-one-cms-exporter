### All-in-one Cloud CMS exporter

> 无状态服务，实现了如下
>
> - 阿里云 / ali
> - 腾讯云 / tc
> - AWS / aws (暂未实现)
> - Google / google
> - Megaport / vxc
>
> 云厂商namespace 的 监控指标拉取功能；基于 remote write 推送后端 TSDB

#### 为什么要做 All-in-one Cloud CMS exporter

```text
因为市面上 cloud exporter 无法满足需求

开源cloud cms exporter 存在的问题
1. 采集周期调整需要修改配置,周期与exporter耦合;
2. 指标和region需要手动指定；多指标多region场景配置麻烦；
3. 有状态服务，无法高可用，单点问题；多副本重复采集；
4. 每个云厂商每个子账号都单独维护一个exporter,组件分散不好管理；
5. 默认产品指标仅携带instance_id，缺少源数据标签关联，指标信息缺失，问题定位不清晰；
6. 基于配置文件显式声明 cloud skey sid，存在安全风险；
7. ...


All-in-one Cloud CMS exporter 解决了哪些问题
1. 调用周期 账密文件 产品ns 与 exporter 解耦，独立组件cycle（暂未开源; 有疑问提issue）负责调度和配置分发；
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
>   batch: 5000                     # 每次发往后端的series数量
>   remote_write:
>     - url: http://172.19.64.103:19000/prometheus/v1/write
>       remote_timeout: 10s
>       write_relabel_configs:
>         - source_labels: ["__name__"]
>           regex: "^net.+"
>           action: "keep"
> provider:
>   ali: # 当前仅支持 ali 定制化配置
>     # 阿里云是否使用 batchGet 拉取方式;默认采用 DescribeMetricLastData
>     # BatchGet 付费模式拉取，不支持提前聚合 (Dimensions/Gruop by) 等配置
>     # DescribeMetricLast 定量免费额度拉取，建议开通按量付费
>     # 建议使用 DescribeMetricLast，因为 BatchGet 只保证数据完整性，但也无法做到实时数据落盘拉取；云产品数据产生->云监控需要min级落盘时延
>     batch_get_enabled: false
>
> http:
>   listen: :8081
>   timeout: 35s					# 接口即成了 pprof, timeout 太低会导致 pprof timeout
> ```

#### 项目运行说明

> 项目通过 Cycle(自研为开源) 组件调度运行，单独部署无效果；
>
> 或 向指定 url path 发送任务驱动 post 请求也可以模拟 Cycle 调度；各云厂商需要传入的参数不同，源码有体现