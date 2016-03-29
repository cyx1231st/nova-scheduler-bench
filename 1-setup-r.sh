#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

if ! [ $# -eq 3 ]
then
    echo "setup.sh <number of schedulers> <number of nodes> <number of max requests>" 1>&2
    exit 1
fi

schedulers=$1
nodes=$2
max=$3

./openstack-bench/run_suite.sh $schedulers 0
python queue_requests.py $nodes $max

popd > /dev/null
