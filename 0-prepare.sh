#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

pushd "/opt/devstack/" > /dev/null
source openrc admin admin
popd > /dev/null

tenant_id=`keystone tenant-list | grep " admin" | sed -e "s/ |       admin        |   True  |//" | sed -e "s/| //"` > /dev/null
echo "Tenant ID: $tenant_id"

nova quota-update --cores 2147483640 --instances 2147483640 --ram 2147483640 $tenant_id

nova flavor-create f1 11 64 64 1
nova flavor-create f2 12 1024 256 1
nova flavor-create f3 13 4096 1024 2
nova flavor-create f4 14 8192 2048 4
nova flavor-create f5 15 16384 4096 8
nova flavor-create f6 16 32768 8192 16

popd > /dev/null
