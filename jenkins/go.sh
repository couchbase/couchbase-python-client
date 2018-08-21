#!/usr/bin/env bash
export args=$@
echo "LCB_INC ${LCB_INC}"
echo "LCB_LIB ${LCB_LIB}"

export VENVDIR=~/virtualenvs/${PYVERSION}_LCB_${LCB_VERSION}

build_lcb(){
    rm -rf $LCB_INSTALL
    mkdir -p $LCB_INSTALL
    pushd .
    rm -rf $LCB_DIR
    git clone http://review.couchbase.org/libcouchbase $LCB_DIR
    cd $LCB_DIR
    git checkout $LCB_VERSION
    mkdir $LCB_BUILD
    cd $LCB_BUILD
    ../cmake/configure --enable-debug --prefix $LCB_INSTALL
    make install
    popd
}

activate_environment(){
    source $VENVDIR/bin/activate
    export LD_LIBRARY_PATH=$LCB_LIB:$LD_LIBRARY_PATH
}

build_environment(){
    build_lcb
    rm -Rf $VENVDIR
    virtualenv -p `which python$PYVERSION` $VENVDIR
    activate_environment
    rm -Rf build
    export COMMAND="python setup.py build_ext --inplace --library-dirs ${LCB_LIB} --include-dirs ${LCB_INC}"
    echo $COMMAND
    eval $COMMAND
    python setup.py install
    cat dev_requirements.txt | xargs -n 1 pip install
    #pip install -r dev_requirements.txt
    cp tests.ini.sample tests.ini
    sed -i -e '0,/enabled/{s/enabled = True/enabled = False/}' tests.ini
}

run_all_nosetests(){
    if [ -z ${PYCBC_DEBUG_SYMBOLS} ] && [ -z ${PYCBC_DEBUG} ]; then
        nosetests --with-xunit -v
    else
        export TMPCMDS="${PYVERSION}_${LCB_VERSION}_cmds"
        echo "trying to write to: ["
        echo "${TMPCMDS}"
        echo "]"
        echo "run `which nosetests` -v --with-xunit" > "${TMPCMDS}"
        echo "bt" >>"${TMPCMDS}"
        echo "py-bt" >>"${TMPCMDS}"
        echo "quit" >>"${TMPCMDS}"
        gdb -batch -x "${TMPCMDS}" `which python`
    fi


}
export command=$1

build_environment
source $VENVDIR/bin/activate
export LD_LIBRARY_PATH=$LCB_LIB:$LD_LIBRARY_PATH

echo "PYCBC_VALGRIND ${PYCBC_VALGRIND}"

if [ -n "${PYCBC_VALGRIND}" ]; then
    export CMDARGS=${args[@]:1}
    export VALGRIND_REPORT_DIR="build/valgrind/${PYCBC_VALGRIND}"
    mkdir -p $VALGRIND_REPORT_DIR
    valgrind --suppressions=jenkins/suppressions.txt --gen-suppressions=all --track-origins=yes --leak-check=full --xml=yes --xml-file=${VALGRIND_REPORT_DIR}/valgrind.xml --show-reachable=yes ${CMDARGS} `which python` `which nosetests` -v "${PYCBC_VALGRIND}" > build/valgrind.txt
fi
if [ -z ${command} ]; then
    #echo "skipping nosetests"
    run_all_nosetests
fi
