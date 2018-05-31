#!/usr/bin/env bash
echo "LCB_INC ${LCB_INC}"
echo "LCB_LIB ${LCB_LIB}"

export VENVDIR=~/virtualenvs/${PYVERSION}_LCB_${LCB_VERSION}
rm -Rf $VENVDIR
virtualenv -p `which python$PYVERSION` $VENVDIR
source $VENVDIR/bin/activate
export LD_LIBRARY_PATH=$LCB_LIB:$LD_LIBRARY_PATH



rm -Rf build
export COMMAND="python setup.py build_ext --inplace --library-dirs ${LCB_LIB} --include-dirs ${LCB_INC}"
echo $COMMAND
eval $COMMAND
python setup.py install
pip install -r dev_requirements.txt

export TMPCMDS="${PYVERSION}_${LCB_VERSION}_cmds"
echo "trying to write to: ["
echo "${TMPCMDS}"
echo "]"
echo "run `which nosetests` -v --with-xunit" > "${TMPCMDS}"
echo "bt" >>"${TMPCMDS}"
echo "quit" >>"${TMPCMDS}"

cp tests.ini.sample tests.ini
sed -i -e '0,/enabled/{s/enabled = True/enabled = False/}' tests.ini
if [ -n $PYCBC_VALGRIND ]; then
    valgrind --track-origins=yes --leak-check=full --show-reachable=yes `which python` `which nosetests` -v "${PYCBC_VALGRIND}" > build/valgrind.txt
fi
if [ -z $PYCBC_DEBUG_SYMBOLS ] && [ -z $PYCBC_DEBUG ]; then
	nosetests --with-xunit -v
else
gdb -batch -x "${TMPCMDS}" `which python`
fi
