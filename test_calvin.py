import subprocess
import time
from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove
from datetime import datetime

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


def update_settings(nodes,wh):
    print("Updating config and building...")
    update_wh(wh)
    subprocess.run(['./propogate_settings.sh',str(nodes)])
    rename_portfile()
    print("Build finished, start running...")


def test_run_once(nodes,per_distr,wh,t_out=90):
    print("|Config|nnodes:{}|dist:{}|WH:{}".format(nodes,per_distr,wh))
    try:
        rename_portfile()
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

def rename_portfile():
    with open('./src/deployment/portfile','w') as f:
        f.write('54564')
    time.sleep(3)


if __name__ == "__main__":

    # for nodes in [1,2,4,8,16,32]:
    #     for warehouse in [1,2,4,8,16,32,64,128]:
    #         update_settings(nodes,warehouse)
    #         for dist in [0,10,20,30,40,50,60,70,80,90,100]:
    #             print(datetime.now())
    #             test_run_once(nodes,dist,warehouse)
    

    for nodes in [32]:
        for warehouse in [8]:
            update_settings(nodes,warehouse)
            for dist in [0]:
                test_run_once(nodes,dist,warehouse)

