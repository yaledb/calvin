#!/bin/bash

mkdir -p ~/db3
sudo chown -R $(whoami) ~/db3

mv ~/calvin/tpcc.h ~/calvin/src/applications

cd src
make -j
