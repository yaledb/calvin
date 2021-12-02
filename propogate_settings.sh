#!/bin/bash

nnodes=$1

uname=$(whoami)
./generate_deploy_run.sh $nnodes

cd ~/calvin/src
make -j

cd ..

# echo "# Node<id>=<replica>:<partition>:<cores>:<host>:<port>" > $fname
for (( i = 1; i < nnodes; i++ )); do
# scp ./compile_calvin.sh ${uname}@node${i}:~/calvin
scp ./deploy-run.conf ${uname}@node${i}:~/calvin
scp ./src/applications/tpcc.h ${uname}@node${i}:~/calvin
sleep 1
ssh -p 22 ${uname}@node${i} "cd ~/calvin && sudo ./compile_calvin.sh"
done