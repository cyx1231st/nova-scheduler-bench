#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

for file in `ls ./runtime_logs`; do
    > "./runtime_logs/$file"
done

popd > /dev/null
