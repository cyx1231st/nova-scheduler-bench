#!/bin/bash
set -x
sudo killall nova-scheduler
sudo killall nova-api
sudo killall nova-conductor
sudo killall nova-compute
sudo pkill -f "python run_bench.py"
