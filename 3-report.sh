#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
python parse.py $1 openstack-bench/$result_folder

popd > /dev/null
