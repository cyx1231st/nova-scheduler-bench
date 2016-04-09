#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

pushd "/opt/devstack/" > /dev/null
source openrc admin admin
popd > /dev/null
echo "Sourced openrc"

tenant_id=`keystone tenant-list | grep " admin" | sed -e "s/ |       admin        |   True  |//" | sed -e "s/| //"` > /dev/null
echo "Got tenant id: $tenant_id"

nova quota-update --cores 2147483640 --instances 2147483640 --ram 2147483640 $tenant_id
echo "Quota updated"

nova flavor-create f1 11 64 64 1
echo "Added flavor id: 11"
nova flavor-create f2 12 1024 256 1
echo "Added flavor id: 12"
nova flavor-create f3 13 4096 1024 2
echo "Added flavor id: 13"
nova flavor-create f4 14 8192 2048 4
echo "Added flavor id: 14"
nova flavor-create f5 15 16384 4096 8
echo "Added flavor id: 15"
nova flavor-create f6 16 32768 8192 16
echo "Added flavor id: 16"

echo "Done"
popd > /dev/null
