#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

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

counter=0
for id in `nova list | awk '$2 && $2 != "ID" {print $2}'`
do
    curl -i http://localhost:8774/v2.1/$TENANT/servers/$id -X DELETE -H "X-Auth-Project-Id: admin" -H "Content-Type: application/json" -H "Accept: application/json" -H "X-Auth-Token: $OS_TOKEN" 1>/dev/null 2>&1 &
    echo "Sent request to delete $id"
    let counter=counter+1
done
echo "Sent $counter requests"

wait_time=$((counter/20))
for (( c=$wait_time; c>=0; c-- ))
do
    echo "Sleep $c seconds for nova list"
    sleep 1
done

watch -n 5 "nova list"

popd > /dev/null
