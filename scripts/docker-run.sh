#!/bin/bash

source /etc/profile

DOCKER_HARBOR="boker-hub-registry.cn-shanghai.cr.aliyuncs.com/ops/watcher4metrics"
TAG="v0.9.0"
LANG_TAG="${DOCKER_HARBOR}:${TAG}"

# 停止原容器
#docker rm -f "$(docker ps | grep ${DOCKER_HARBOR} | grep ${TAG} | awk '{print $1}')" > /dev/null

docker build -t ${LANG_TAG} . \
&& docker push ${LANG_TAG} \
&& kubectl apply -f ../build/kubernetes/watcher4metrics-k8s.yml

#&& docker run -d --name ${TAG} \
#-p 8081:8081 \
#-e WATCHER4METRICS_ENV="test" \
#-m 300m \
#--restart=always \
#--add-host=api.bokecorp.com:139.224.236.74 \
#--log-driver=loki \
#--log-opt loki-url="http://172.18.12.38:3100/loki/api/v1/push" \
#--log-opt max-size=100m \
#--log-opt max-file=20 \
#$(docker images | grep ${DOCKER_HARBOR} | grep ${TAG} | awk '{print $3}')
