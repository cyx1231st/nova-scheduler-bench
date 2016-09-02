#!/bin/bash
TOP_DIR=$(cd $(dirname "$0") && pwd)
pushd $TOP_DIR > /dev/null

result_folder="results"
runtime_folder="runtime_logs"

mkdir $result_folder
rm $result_folder/*
cp -r ./$runtime_folder/* ./$result_folder
python parse.py $result_folder

echo "Done!"

popd > /dev/null
