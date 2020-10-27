#!/bin/bash

./build/simulator -z 0 -c 0 > load_zipf_0_cache_0
./build/simulator -z 0 -c 10000 > load_zipf_0_cache_10000
./build/simulator -z 90 -c 0 > load_zipf_90_cache_0
./build/simulator -z 90 -c 10000 > load_zipf_90_cache_10000
./build/simulator -z 95 -c 0 > load_zipf_95_cache_0
./build/simulator -z 95 -c 10000 > load_zipf_95_cache_10000
./build/simulator -z 99 -c 0 > load_zipf_99_cache_0
./build/simulator -z 99 -c 10000 > load_zipf_99_cache_10000
