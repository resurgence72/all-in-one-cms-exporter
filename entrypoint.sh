#!/bin/bash

# n9e 地址
N9E_SERVER=$N9E_SERVER
if [ "x${N9E_SERVER}" == "x" ]; then
  N9E_SERVER="http://api.bokecorp.com/n9e-v5-server/openfalcon/push"
fi
sed -i 's#(n9e_server: )(.*)#\1${N9E_SERVER}#g' /opt/app/watcher4metrics.yml

# n9e gateway auth-header
AUTH_TOKEN=$AUTH_TOKEN
sed -i 's#(value: )(.*)#\1${AUTH_TOKEN}#g' /opt/app/watcher4metrics.yml

# watcher addr
NODATA_NOTIFY=$NODATA_NOTIFY
if [ "x${NODATA_NOTIFY}" == "x" ]; then
  NODATA_NOTIFY="http://127.0.0.1:8080/api/v1/notify"
fi
sed -i 's#(watcher_http_server: )(.*)#\1${NODATA_NOTIFY}#g' /opt/app/watcher4metrics.yml

/opt/app/watcher4metrics -config /opt/app/watcher4metrics.yml
