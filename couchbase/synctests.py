from couchbase.tests.base import ClusterInformation
from couchbase.connection import Connection
from couchbase.views.iterator import View
from couchbase.tests import base

# Set the default Connection class to use.
# I've modified the ClusterInformation class in tests.base to
# have a class-level field defining this.
base.DEFAULT_CONNECTION_CLASS = Connection
base.DEFAULT_VIEW_CLASS = View

from couchbase.tests.cases import *

# import tests files.. (this is a simplification, I could for example,
# scan the tests directory and use __import__ or similar to do so
# manually)

# EXPECTED: the test classes defined in 'test_append' run
# RESULT: nothing runs. no errors.
