#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

if [ $# -eq 2 ]
then
    mode="periodical"
    r_count=$1
    s_time=$2
elif [ $# -eq 0 ]
then
    mode="normal"
else
    echo "setup.sh [<concurrency> <sleep time>]" 1>&2
    exit 1
fi

pushd "/opt/devstack/" > /dev/null
source openrc admin admin
popd > /dev/null
echo "Sourced openrc"

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
echo "Got token $OS_TOKEN"

TENANT=`openstack project list | grep " admin" | sed -e "s/ | admin              |//" | sed -e "s/| //"` > /dev/null
echo "Got tenant id: $TENANT"

IMAGE=`openstack image list | grep "uec " | sed -e "s/ | cirros-0.3.4-x86_64-uec         | active |//" | sed -e "s/| //"`
echo "Got image id: $IMAGE"

counter=0
if [ -e .requests ]
then
    result_folder="runtime_logs"
    mkdir -p $result_folder
    rm $result_folder/*

    source .requests
    for i in "${request_list[@]}"
    do
        curl -i http://localhost:8774/v2.1/$TENANT/servers -X POST -H "X-Auth-Project-Id: admin" -H "Content-Type: application/json" -H "Accept: application/json" -H "X-Auth-Token: $OS_TOKEN" -d '{"server": {"name": "p'$counter'", "imageRef": "'$IMAGE'", "flavorRef": "'${request_list[counter]}'", "max_count": 1, "min_count": 1}}' 1>/dev/null 2>&1 &
        echo "Sent a request p$counter with flavor ${request_list[counter]}"
        let counter=counter+1
	
        if [ $mode == "periodical" ]
        then
	    let rem=$counter%$r_count
	    if [ $rem = 0 ]; then
                echo "Sleep $s_time seconds"
                sleep $s_time
            fi
        fi
    done
fi

watch "python parse.py ./runtime_logs --brief"

echo "Done, requested $counter instances!"

popd > /dev/null
