#!/bin/bash

nnodes=32

cd ~/calvin/src && make clean
rm -r ~/calvin/src
# cp -a ~/calvin/src_traditional_2pl_2pc ~/calvin/src
cp -a ~/calvin/src_calvin ~/calvin/src

for (( i = 1; i < nnodes; i++ )); do
# ssh -p 22 miaoyu@node${i} "sudo rm -r ~/calvin"
# ssh -p 22 miaoyu@node${i} "git clone https://github.com/sam1016yu/calvin.git"
# sleep 3
ssh -p 22 miaoyu@node${i} "sudo chown -R miaoyu ~/calvin"

# # ssh -p 22 miaoyu@node${i} "[ ! -d '~/calvin/src_calvin'] && mv ~/calvin/src/ ~/calvin/src_calvin/ "

ssh -p 22 miaoyu@node${i} "cd ~/calvin/src && make clean"
ssh -p 22 miaoyu@node${i} "rm -r  ~/calvin/src"
sleep 1
ssh -p 22 miaoyu@node${i} "cp -a ~/calvin/src_calvin ~/calvin/src"
# ssh -p 22 miaoyu@node${i} "cp -a ~/calvin/src_traditional_2pl_2pc ~/calvin/src"
# ssh -p 22 miaoyu@node${i} "cd calvin && ~/calvin/install-ext"
# scp -r ext miaoyu@node${i}:~/calvin
done