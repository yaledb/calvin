#!/bin/bash

nnodes=$1
port=$2

uname=$(whoami)
./generate_deploy_run.sh $nnodes ${port}

mkdir -p ~/db3/
cd ~/calvin/src
make -j

cd ..

single_scp () {
	i=$1
	scp ./deploy-run.conf ${uname}@node${i}:~/calvin
	scp -r ./src/ ${uname}@node${i}:~/calvin
	#scp ./src/applications/tpcc.h ${uname}@node${i}:~/calvin
	#scp ./src/applications/tpcc.cc ${uname}@node${i}:~/calvin
	#scp ./src/backend/simple_storage.h ${uname}@node${i}:~/calvin
	#scp ./src/backend/simple_storage.cc ${uname}@node${i}:~/calvin
	#scp ./src/sequencer/sequencer.h ${uname}@node${i}:~/calvin
}

for (( i = 1; i < nnodes; i++ )); do
	single_scp $i &
	#scp ./deploy-run.conf ${uname}@node${i}:~/calvin
	#scp ./src/applications/tpcc.h ${uname}@node${i}:~/calvin
	#scp ./src/applications/tpcc.cc ${uname}@node${i}:~/calvin
done

wait

# echo "# Node<id>=<replica>:<partition>:<cores>:<host>:<port>" > $fname
for (( i = 1; i < nnodes; i++ )); do
# scp ./compile_calvin.sh ${uname}@node${i}:~/calvin
	ssh -p 22 ${uname}@node${i} "cd ~/calvin && ./compile_calvin.sh" &
	#ssh -p 22 ${uname}@node${i} "cd ~/calvin && sudo ./compile_calvin.sh"
done

wait
