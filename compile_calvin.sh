#!/bin/bash

mkdir -p ~/db3
sudo chown -R miaoyu ~/db3

cp -a src_calvin src
mv ~/calvin/tpcc.h ~/calvin/src/applications

cd src
make -j


# echo 'LD_LIBRARY_PATH="/users/miaoyu/calvin/ext/protobuf/src/.libs:/users/miaoyu/calvin/ext/zookeeper/.libs:/users/miaoyu/calvin/ext/googletest/lib/.libs:/usr/local/lib:/usr/lib:/users/miaoyu/calvin/ext/zeromq/lib"' >> /etc/environment
# source /etc/environment