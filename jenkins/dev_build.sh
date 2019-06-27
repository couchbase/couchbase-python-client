#!/usr/bin/env bash



if [ "$1" = "rebuild" ]; then
echo Rebuilding
rm -rf build couchbase/*.so* couchbase/*.dylib*
rm -rf build couchbase_core/*.so* couchbase_core/*.dylib*
fi;
python setup.py build_ext --debug --inplace &> build.txt &
