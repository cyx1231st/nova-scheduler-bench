#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

for file in `ls ./openstack-bench/results`; do
    > "./openstack-bench/results/$file"
done

popd > /dev/null
