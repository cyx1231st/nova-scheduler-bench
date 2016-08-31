#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

./scripts/killall.sh >/dev/null 2>&1 &
rm .requests >/dev/null 2>&1 &

mysql --database='nova' --execute=\
"delete from services where services.binary='nova-compute';
delete from compute_nodes;
delete from services where services.binary='nova-scheduler';"

watch "ps aux | grep run_bench"

popd > /dev/null
