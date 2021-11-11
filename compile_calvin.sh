#!/bin/bash

mkdir -p ~/db3
sudo chown -R miaoyu ~/db3

# cp -a src_calvin src
mv ~/calvin/tpcc.h ~/calvin/src/applications

cd src
make -j