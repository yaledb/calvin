#!/bin/bash

nnodes=32

uname=$(whoami)

rm -r ~/calvin/bin ~/calvin/db ~/calvin/obj ~/calvin/src

# cp -a ~/calvin/src_traditional_2pl_2pc ~/calvin/src

#*****************CHOOSE ONE************************
#------------ Run calvin-------------------------#
DIR_SRC=~/calvin/src_calvin
#-------------Run 2PL----------------------------#
#DIR_SRC=~/calvin/src_traditional_2pl_2pc
#**************************************************

cp -a $DIR_SRC ~/calvin/src

single_scp () {
	i=$1
	ssh -p 22 ${uname}@node${i} "sudo chown -R ${uname} ~/calvin"
	ssh -p 22 ${uname}@node${i} "rm -r ~/calvin/bin ~/calvin/db ~/calvin/obj ~/calvin/src"
	ssh -p 22 ${uname}@node${i} "cp -a ${DIR_SRC} ~/calvin/src"
}

for (( i = 1; i < nnodes; i++ )); do
	single_scp $i &
	#ssh -p 22 ${uname}@node${i} "sudo chown -R ${uname} ~/calvin"
	#ssh -p 22 ${uname}@node${i} "rm -r ~/calvin/bin ~/calvin/db ~/calvin/obj ~/calvin/src"
	#ssh -p 22 ${uname}@node${i} "cp -a ${DIR_SRC} ~/calvin/src"
done

wait