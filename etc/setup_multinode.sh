#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

if ! [ $# -eq 1 ]
then
    echo "setup-multinode.sh <remote>" 1>&2
    exit 1
fi

hostname=$1

cp ./1-setup-r.sh ../
cp ./3-report-r.sh ../
cp ./5-reset-r.sh ../
cp helper-clear-log.sh ../
cp run_suite_r.sh ../openstack-bench/

sed -i -e "s/usr@host/$hostname/g" ../3-report-r.sh
sed -i -e "s/usr@host/$hostname/g" ../5-reset-r.sh

echo "Done!"

popd > /dev/null
