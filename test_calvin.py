import subprocess
import time

def test_run_once(per_distr):
    print("Config#"+per_distr)
    try:
        rename_portfile()
        subprocess.run(
        [   './bin/deployment/cluster', 
            "-c", "./deploy-run.conf",
            '-p','./src/deployment/portfile',
            '-d','./bin/deployment/db',
            't',
            per_distr
            ], timeout=90
        )
    except subprocess.TimeoutExpired:
        print('Timeout! Starting new run...')

def rename_portfile():
    with open('./src/deployment/portfile','w') as f:
        f.write('54564')
    time.sleep(3)


if __name__ == "__main__":
    test_config = [0,5,10,15,20,30,40,50,60,70,80,90,100]
    for config in test_config:
        test_run_once(str(config))

