#!/bin/bash

mkdir -p ~/db3
ls ~/
#sudo chown -R ${uname} ~/db3

mv ~/calvin/tpcc.h ~/calvin/src/applications

cd src
make -j
