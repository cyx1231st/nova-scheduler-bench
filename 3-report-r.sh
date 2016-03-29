#!/bin/bash

set -x

TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
mkdir $result_folder
rm $result_folder/*
cp -r openstack-bench/$result_folder/* ./$result_folder
scp yingxin@10.239.48.9:~/openstack-bench/openstack-bench/$result_folder/* ./$result_folder
python parse.py --offset $1 $result_folder

popd > /dev/null
