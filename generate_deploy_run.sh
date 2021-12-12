#!/bin/bash

nnodes=$1
port=$2

fname="deploy-run.conf"
# fname="deploy-run.conf.test"

echo "# Node<id>=<replica>:<partition>:<cores>:<host>:<port>" > $fname
for (( i = 0; i < nnodes; i++ )); do
	node=$(( i+1 ))
	echo "node${i}=0:${i}:${nnodes}:10.10.1.${node}:${port}" >> $fname
done
