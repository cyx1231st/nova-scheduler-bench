#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

if [ $# -eq 2 ]
then
    nodes_start=$1
    nodes_end=$2
elif [ $# -eq 1 ]
then
    nodes_start=0
    nodes_end=$1
else
    echo "run_suite_r.sh [<start_node>] <end_node>" 1>&2
    exit 1
fi

result_folder="results"
mkdir -p $result_folder

for (( i=$nodes_start; i<$nodes_end; i++ ))
do
python run_bench.py --result-folder $result_folder --host node$i compute >/dev/null 2>&1 &
echo "launched compute node node$i"
done

popd > /dev/null
