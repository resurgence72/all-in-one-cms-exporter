### watcher4metrics 
### All-in-one Cloud CMS exporter
> 无状态服务，实现了如下
>
> - 阿里云 / ali
> - 腾讯云 / tc
> - AWS / aws
> - Google / google
> - Megaport / megaport
>
> 云厂商自定义 namespace 的 metrics 拉取功能； 将数据基于 remote write 推送之后端 TSDB



### 1. 配置文件说明

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



### 2. watcher4metrics 服务部署说明

#### 1. 二进制部署

> ```shell
> # 创建二进制部署目录
> mkdir -p /data/service
> 
> # 构建二进制文件
> cd /data/watcher4metrics_project/cmd/watcher4metrics/ && go build -o watcher4metrics .
> 
> # 拷贝至部署目录
> \cp watcher4metrics watcher4metrics.yml /data/service
> 
> # 拷贝service文件
> \cp /data/watcher4metrics_project/etc/service/watcher4metrics.service /etc/systemd/system/watcher4metrics.service
> 
> # 运行watcher4metrics
> systemctl start watcher4metrics
> 
> PS: 也可以使用supervisor部署方式
> ```



#### 2. Docker 容器化部署

> ```sh
> cd /data/watcher4metrics_project/
> 
> # 一键build push run
> sh scripts/docker-run.sh
> 
> # 查看容器运行日志
> docker logs -f xxx | egrep -v 'GIN'
> ```



#### 3. Kubernetes 部署

> 暂未同步