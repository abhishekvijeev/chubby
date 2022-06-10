#!/bin/bash

# config replica topology in PD
# see https://github.com/tikv/website/blob/3795d106b0b2b8e9c75b9a86f0bf18dd96d59727/content/docs/dev/tasks/configure/topology.md
nohup ./pd-server --name=pd --data-dir=/tmp/pd/data --client-urls="http://127.0.0.1:2379" --peer-urls="http://127.0.0.1:2380" --initial-cluster="pd=http://127.0.0.1:2380" --log-file=/tmp/pd/log/pd.log --config topo.yaml > pd-server.log  2>&1 &

nohup ./tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20160" --data-dir=/tmp/tikv/data --log-file=/tmp/tikv/log/tikv.log > tikv-server-0.log 2>&1 &
