#!/bin/bash

sudo echo 'LD_LIBRARY_PATH="/users/miaoyu/calvin/ext/protobuf/src/.libs:/users/miaoyu/calvin/ext/zookeeper/.libs:/users/miaoyu/calvin/ext/googletest/lib/.libs:/usr/local/lib:/usr/lib:/users/miaoyu/calvin/ext/zeromq/lib"' >> /etc/environment
sudo source /etc/environment