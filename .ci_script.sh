#!/bin/sh
set -e
set -x

# Ensure it loads OK
python -m couchbase.connection

# Set up our test directory..
cp .tests.ini.travis tests.ini

nosetests -v couchbase.tests.test_sync
nosetests -v gcouchbase.tests.test_api || echo "Gevent tests failed"
nosetests -v txcouchbase || echo "Twisted tests failed"
