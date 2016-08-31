#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

for file in `ls ./openstack_bench/openstack_patcher/results`; do
    > "./openstack_bench/openstack_patcher/results/$file"
done

popd > /dev/null
