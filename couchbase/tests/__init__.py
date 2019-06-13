from couchbase_tests import caseslist

import couchbase_v2.tests.cases

caseslist += [couchbase_v2.tests.cases]

import couchbase.tests_v3.cases as latest_cases

caseslist += [latest_cases]

import couchbase_tests.test_sync as test_sync
