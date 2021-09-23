#!/bin/bash

nnodes=$1

./generate_deploy_run.sh $nnodes

cd ~/calvin/src
make -j

cd ..

# echo "# Node<id>=<replica>:<partition>:<cores>:<host>:<port>" > $fname
for (( i = 1; i < nnodes; i++ )); do
scp ./compile_calvin.sh miaoyu@node${i}:~/calvin
scp ./deploy-run.conf miaoyu@node${i}:~/calvin
scp ./src/applications/tpcc.h miaoyu@node${i}:~/calvin
sleep 1
ssh -p 22 miaoyu@node${i} "cd ~/calvin && sudo ./compile_calvin.sh"
done