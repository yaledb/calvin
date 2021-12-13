#!/usr/bin/env python3

import subprocess
import time
from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove
from datetime import datetime
import random

def update_wh(wh):
    #Create temp file
    fh, abs_path = mkstemp()
    file_path = "./src/applications/tpcc.h"
    with fdopen(fh,'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                if line.startswith("#define WAREHOUSES_PER_NODE"):
                    line = "#define WAREHOUSES_PER_NODE {}\n".format(wh)
                new_file.write(line)
    #Copy the file permissions from the old file to the new file
    copymode(file_path, abs_path)
    #Remove original file
    remove(file_path)
    #Move new file
    move(abs_path, file_path)


def update_settings(nodes,wh,port):
    print("Updating config and building...")
    update_wh(wh)
    subprocess.run(['./propogate_settings.sh',str(nodes),str(port)])
    rename_portfile(port)
    time.sleep(3)
    print("Build finished, start running...")


def test_run_once(nodes,per_distr,wh,t_out=90):
    print("|Config|nnodes:{}|dist:{}|WH:{}".format(nodes,per_distr,wh))
    try:
        # rename_portfile()
        subprocess.run(
        [   './bin/deployment/cluster', 
            "-c", "./deploy-run.conf",
            '-p','./src/deployment/portfile',
            '-d','./bin/deployment/db',
            't',
            str(per_distr),
            # '2>&1','|','tee','-a','./test_{}'.format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
            ], timeout=t_out
        )
    except subprocess.TimeoutExpired:
        print('Timeout! Starting new run...')

def rename_portfile(port):
    with open('./src/deployment/portfile','w') as f:
        f.write(str(port))
    #time.sleep(3)

def gen_port_list(test_list):
    length = sum([len(dist) for node_wh, dist in test_list])
    # currently we assume the number of experiment won't exceed 5000
    # port_list = [ 50000 + 2*x for x in random.sample(range(length), length)]
    port_list = random.sample(range(50000, 60000), length)
    return port_list

if __name__ == "__main__":

    # for nodes in [1,2,4,8,16,32]:
    #     for warehouse in [1,2,4,8,16,32,64,128]:
    #         update_settings(nodes,warehouse)
    #         for dist in [0,10,20,30,40,50,60,70,80,90,100]:
    #             print(datetime.now())
    #             test_run_once(nodes,dist,warehouse)
    

    #for nodes in [32]:
    #    for warehouse in [8]:
    #        update_settings(nodes,warehouse)
    #        for dist in [0]:
    #            test_run_once(nodes,dist,warehouse)

    # test with abnormal configs
    # list: [(node, wh), [dist1, dist2, ..], ...]
    # test_list = [[(4, 4), [70]]]
    # test_list = [[(16,4), [90, 100]], [(4, 4), [70]], [(32,4), [80, 90, 100]]]
    #test_list = [[(16,4), [60, 70, 80, 90, 100]], [(4, 4), [50, 60, 70, 80, 90]], [(32,4), [50, 60, 70, 80, 90, 100]]]
    #test_list = [[(32,4), [70, 90]]]
    #test_list = [[(32,32), [60, 70]]]
    test_list = [[(32,32), [90]]]
    port_list = gen_port_list(test_list)
    index = 0
    for elem in test_list:
        nodes = elem[0][0]
        wh = elem[0][1]
        #update_settings(nodes, wh)
        for dist in elem[1]:
            port = port_list[index]
            index = index+1
            print("Run exp with setting[node:{0}, wh:{1}, dist:{2}, port:{3}]".format(nodes, wh, dist, port))
            update_settings(nodes, wh, port)
            test_run_once(nodes, dist, wh)
            
            
    
