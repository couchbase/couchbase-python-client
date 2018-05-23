export VENVDIR=~/virtualenvs/${PYVERSION}_LCB_${LCB_VERSION}
rm -Rf $VENVDIR
virtualenv -p `which python$PYVERSION` $VENVDIR
source $VENVDIR/bin/activate

rm -Rf build
python setup.py build_ext --inplace --library-dir $LCB_LIB --include-dir $LCB_INC
python setup.py install
pip install -r dev_requirements.txt
cp tests.ini.sample tests.ini
sed -i -e '0,/enabled/{s/enabled = True/enabled = False/}' tests.ini
if [ -z $PYCBC_DEBUG_SYMBOLS ]
then
	nosetests --with-xunit -v
else
set TMPCMDS=${PYVERSION}_${LCB_VERSION}_cmds
	echo "run `which nosetests` -v --with-xunit">$TMPCMDS
	echo "bt" >>$TMPCMDS
	echo "quit" >>$TMPCMDS
gdb -batch -x $TMPCMDS `which python`
fi
