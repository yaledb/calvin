#!/bin/bash

nnodes=32



rm -r ~/calvin/bin ~/calvin/db ~/calvin/obj ~/calvin/src

# cp -a ~/calvin/src_traditional_2pl_2pc ~/calvin/src

#*****************CHOOSE ONE************************
#------------ Run calvin-------------------------#
DIR_SRC=~/calvin/src_calvin
#-------------Run 2PL----------------------------#
# DIR_SRC=~/calvin/src_traditional_2pl_2pc
#**************************************************

cp -a $DIR_SRC ~/calvin/src

for (( i = 1; i < nnodes; i++ )); do
ssh -p 22 miaoyu@node${i} "sudo chown -R miaoyu ~/calvin"
ssh -p 22 miaoyu@node${i} "rm -r ~/calvin/bin ~/calvin/db ~/calvin/obj ~/calvin/src"
sleep 1
ssh -p 22 miaoyu@node${i} "cp -a ${DIR_SRC} ~/calvin/src"
done