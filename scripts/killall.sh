#!/bin/bash
sudo killall nova-scheduler
sudo killall nova-api
sudo killall nova-conductor
sudo killall nova-compute
sudo pkill -f "run_bench.py"
