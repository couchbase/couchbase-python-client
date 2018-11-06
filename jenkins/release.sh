#!/usr/bin/env bash
VER=`basename $PWD`
PYMAJ=3
PYMIN=4
PYVER_SHORT=$PYMAJ$PYMIN
PYVER_LONG=$PYMAJ.$PYMIN
LABEL=all-pythons
export PYCBC_WIN_PROJECT=pycbc-win
#-gerrit-9
mkdir $PYVER_LONG
cd $PYVER_LONG
wget http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win-amd64.zip \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win-amd64-py$PYVER_LONG.exe \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER-cp${PYVER_SHORT}-cp${PYVER_SHORT}m-win-amd64.whl \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x86,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win32.zip \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x86,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win32-py$PYVER_LONG.exe
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER-cp${PYVER_SHORT}-cp${PYVER_SHORT}m-win32.whl \
cd ..
PYMAJ=3
PYMIN=3
PYVER_SHORT=$PYMAJ$PYMIN
PYVER_LONG=$PYMAJ.$PYMIN
mkdir $PYVER_LONG
cd $PYVER_LONG
wget http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win-amd64.zip \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win-amd64-py$PYVER_LONG.exe \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER-cp${PYVER_SHORT}-cp${PYVER_SHORT}m-win-amd64.whl \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x86,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win32.zip \
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x86,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER.win32-py$PYVER_LONG.exe
http://sdkbuilds.sc.couchbase.com/job/$PYCBC_WIN_PROJECT/label=$LABEL,pyarch=x64,pyversion=$PYVER_SHORT/ws/dist/couchbase-$VER-cp${PYVER_SHORT}-cp${PYVER_SHORT}m-win32.whl \
cd ..
find . -name *.exe -exec sh -c 'ln  {} dist/`basename {}` ' \;

