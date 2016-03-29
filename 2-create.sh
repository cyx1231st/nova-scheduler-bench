#!/bin/bash
set -x
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

pushd "/opt/devstack/" > /dev/null
source openrc admin admin
popd > /dev/null

OS_TOKEN=`curl -i \
  -H "Content-Type: application/json" \
  -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "admin",
          "domain": { "id": "default" },
          "password": "123456",
          "tenantName": "admin"
        }
      }
    },
    "scope": {
      "project": {
        "name": "admin",
        "domain": { "id": "default" }
      }
    }
  }
}' \
  http://localhost:5000/v3/auth/tokens | grep X-Subject-Token: | sed -e "s/X-Subject-Token: //"`

TENANT=`keystone tenant-list | grep " admin" | sed -e "s/ |       admin        |   True  |//" | sed -e "s/| //"` > /dev/null

IMAGE=`nova image-list | grep "uec " | sed -e "s/ | cirros-0.3.4-x86_64-uec         | ACTIVE |        |//" | sed -e "s/| //"`

if [ -e .requests ]
then
    ./5-delete.sh

    pushd "openstack-bench" >/dev/null
    result_folder="results"
    mkdir -p $result_folder
    rm $result_folder/*
    popd > /dev/null

    counter=0
    source .requests
    for i in "${request_list[@]}"
    do
        echo "flavor ${request_list[counter]}"
        curl -i http://localhost:8774/v2.1/$TENANT/servers -X POST -H "X-Auth-Project-Id: admin" -H "Content-Type: application/json" -H "Accept: application/json" -H "X-Auth-Token: $OS_TOKEN" -d '{"server": {"name": "p'$counter'", "imageRef": "'$IMAGE'", "flavorRef": "'${request_list[counter]}'", "max_count": 1, "min_count": 1}}' 1>/dev/null 2>&1 &
        let counter=counter+1
    done

    wait_time=$((counter/5))
    for (( c=$wait_time; c>=0; c-- ))
    do
        echo "Sleep $c seconds"
        sleep 1
    done

    watch -n 3 "./3-report.sh --brief"
fi

echo $OS_TOKEN


popd > /dev/null
