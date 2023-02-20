#!/bin/bash
source /etc/profile

DOCKER_HARBOR="xxx.aliyuncs.com/ops/watcher4metrics"
TAG="v0.9.5"
LANG_TAG="${DOCKER_HARBOR}:${TAG}"

docker build -t ${LANG_TAG} . && docker push ${LANG_TAG}

