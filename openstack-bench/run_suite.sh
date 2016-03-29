#!/bin/bash
set -x
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
mkdir -p $result_folder
rm $result_folder/*

./killall.sh

schedulers=$1
nodes=$2

python run_bench.py --result-folder $result_folder api >/dev/null 2>&1 &
python run_bench.py --result-folder $result_folder conductor >/dev/null 2>&1 &

for i in $(seq 1 $schedulers)
do
python run_bench.py --result-folder $result_folder --host sche$i scheduler >/dev/null 2>&1 &
done

for i in $(seq 1 $nodes)
do
python run_bench.py --result-folder $result_folder --host node$i compute >/dev/null 2>&1 &
done

popd > /dev/null
