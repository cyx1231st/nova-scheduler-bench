#!/bin/bash
set -x
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
mkdir -p $result_folder
rm $result_folder/*

./killall.sh

nodes=$1

for i in $(seq 1 $nodes)
do
python run_bench.py --result-folder $result_folder --host node$i compute >/dev/null 2>&1 &
done

popd > /dev/null
