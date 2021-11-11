1. pull repo

2.  run `sudo install-ext` (change uname to your username at top)

----------------------------need to repeat 1 and 2 at every node---------------------


----------------------------------at node 0------------------------------------------

3.  upload your id_rsa to ~/.ssh, chmod to 600 
    3.1 test whether you can ssh to other nodes, e.g., ssh node1

(every time you change protocol)
4.  run `clean_and_copy.sh` with appropriate options,
        DIR_SRC=~/calvin/src_traditional_2pl_2pc  OR DIR_SRC=~/calvin/src_calvin

5.  set the parameters in test_calvin.py in ___main____

6.   run `python3 test_calvin.py`

7.   to record output, run with typescript, e.g., `script -c "python3 test_calvin.py" -q`