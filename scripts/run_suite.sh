#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="../runtime_logs"
mkdir -p $result_folder
rm $result_folder/*

./killall.sh
echo "Killed all modified services"

schedulers=$1
nodes=$2

python ../openstack_bench/openstack_patcher/run_bench.py --result-folder $result_folder api >/dev/null 2>&1 &
python ../openstack_bench/openstack_patcher/run_bench.py --result-folder $result_folder conductor >/dev/null 2>&1 &
echo "Launched api and conductor servcies"

for i in $(seq 1 $schedulers)
do
python ../openstack_bench/openstack_patcher/run_bench.py --result-folder $result_folder --host sche$i scheduler >/dev/null 2>&1 &
echo "Launched scheduler sche$i"
done

for i in $(seq 1 $nodes)
do
python ../openstack_bench/openstack_patcher/run_bench.py --result-folder $result_folder --host node$i compute >/dev/null 2>&1 &
echo "Launched compute node$i"
done

popd > /dev/null
