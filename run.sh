#!/bin/sh

./memcacheq -p 22201 -d -r -c 8000 -m 256 -H ./testenv -L 4096 -N -v > ./testenv.log 2>&1
