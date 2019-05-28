#!/usr/bin/env bash



#export CMAKE_BUILD_DIR=build/temp.macosx-10.12-x86_64-3.6
export CMAKE_BUILD_DIR=cmake-build-debug
if [ "$1" = "rebuild" ]; then
echo Rebuilding
rm -rf build couchbase/*.so* couchbase/*.dylib*
fi;
python setup.py build_ext --debug --inplace --include-dirs ${CMAKE_BUILD_DIR}/install/include/ --library-dirs ${CMAKE_BUILD_DIR}/install/lib/Debug  &> build.txt &
