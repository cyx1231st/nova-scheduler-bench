#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
remote="usr@host"

if ! [ $# -eq 1 ]
then
    echo "report-r.sh <time-offset>" 1>&2
    exit 1
fi

mkdir $result_folder
rm $result_folder/*
cp -r openstack_bench/openstack_patcher/$result_folder/* ./$result_folder
scp $remote:~/nova-scheduler-bench/openstack_bench/openstack_patcher/$result_folder/* ./$result_folder
python parse.py --offset $1 $result_folder
echo "Done"

popd > /dev/null
