#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

./openstack-bench/killall.sh >/dev/null 2>&1 &
ssh yingxin@10.239.48.9 "~/openstack-bench/openstack-bench/killall.sh"
rm .requests >/dev/null 2>&1 &

watch "ps aux | grep run_bench"

popd > /dev/null
