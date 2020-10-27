#!/bin/bash

clean_netcache() {
    rm -rf build
    rm -rf */build
    rm -rf */*/build
    rm -rf cluster/client/build
    rm -rf cluster/client_master/build
    rm -rf cluster/client_slave/build
    rm -rf cluster/backend/build
    rm -rf */lib
    rm -f .DS_Store
    rm -f */.DS_Store
}

clean_netcache

#rsync -az * b26:~/netcache/
#rsync -az * bfn2:~/netcache/

#scp -r * b26:~/xli/netcache/
#scp -r * b30:~/xli/netcache/
scp -r write b14:~/xli/netcache/
scp -r write b30:~/xli/netcache/
scp tools/run.py b30:~/xli/netcache/write/backend
