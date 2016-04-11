#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

pushd "/opt/devstack/" > /dev/null
source openrc admin admin
popd > /dev/null
echo "Sourced openrc"

tenant_id=`openstack project list | grep " admin" | sed -e "s/ | admin              |//" | sed -e "s/| //"` > /dev/null
echo "Got tenant id: $tenant_id"

nova quota-update --cores 2147483640 --instances 2147483640 --ram 2147483640 $tenant_id
echo "Quota updated"

nova flavor-create f1 151 64 64 1
echo "Added flavor id: 151"
nova flavor-create f2 152 1024 256 1
echo "Added flavor id: 152"
nova flavor-create f3 153 4096 1024 2
echo "Added flavor id: 153"
nova flavor-create f4 154 8192 2048 4
echo "Added flavor id: 154"
nova flavor-create f5 155 16384 4096 8
echo "Added flavor id: 155"
nova flavor-create f6 156 32768 8192 16
echo "Added flavor id: 156"

echo "Done"
popd > /dev/null
