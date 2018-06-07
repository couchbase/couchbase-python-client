#!/usr/bin/env bash
export args=$@
eval "${EXTRA_BUILD_ARGS}"
export PYVERSION=${pyversion}
export LCB_VERSION=${lcb_version}

export CURDIR=`readlink -f .`
export LCB_DIR=libcouchbase_${PYVERSION}_${LCB_VERSION}
export LCB_DIR=`readlink -f $LCB_DIR`
export LCB_BUILD=$LCB_DIR/build
export LCB_INSTALL=$CURDIR/root
export LCB_LIB=$LCB_BUILD/lib
export LCB_INC=$LCB_DIR/include:$LCB_BUILD/generated
export LD_LIBRARY_PATH=$LCB_LIB:$LD_LIBRARY_PATH

export PYVER=`echo $PYVERSION | sed -e 's/\.//'`
declare -A PYVERS

export PREFIXES=(["27"]="python27" ["36"]="rh-python36")
export PREFIX="${PREFIXES[PYVER]}"

export UNAMESTR=`uname`
if [[ "$UNAMESTR" == 'Darwin' ]]; then
    export PYCOMMAND=""
else
    export PYCOMMAND="scl enable $PREFIX"
fi
chmod +x ./jenkins/go.sh
${PYCOMMAND} "bash -c ./jenkins/go.sh ${args}"
