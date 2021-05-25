from couchbase_tests import caseslist

import couchbase.tests_v3.cases as latest_cases
import acouchbase.tests.cases as acouchbase_tests
import txcouchbase.tests as txcouchbase_tests

caseslist += [latest_cases, acouchbase_tests, txcouchbase_tests]
import couchbase_tests.test_sync as test_sync
