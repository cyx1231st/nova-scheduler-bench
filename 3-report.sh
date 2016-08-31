#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"

mkdir $result_folder
rm $result_folder/*
cp -r openstack_bench/openstack_patcher/$result_folder/* ./$result_folder
python parse.py $result_folder

echo "Done!"

popd > /dev/null
