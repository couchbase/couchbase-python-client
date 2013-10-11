#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
File which contains all the test cases.
This should be loaded after all the pre-test configuration has
been done.
"""
from __future__ import absolute_import, print_function
import os
import os.path

from couchbase.tests.base import CouchbaseTestCase

imps = []

for name in os.listdir(os.path.dirname(__file__)):
    if not name.startswith('test'):
        continue
    name, ext = os.path.splitext(name)
    if ext.lower() != '.py':
        continue

    imps.append(name)

testmods = []
testclasses = []

for m in imps:
    module = __import__(m,
                        fromlist=['couchbase', 'tests'],
                        globals=globals())
    globals()[m] = module
    testmods.append(module)

from couchbase.tests.admin.test_simple import AdminSimpleTest

for m in testmods:
    for c in dir(m):
        c = getattr(m, c)
        if not isinstance(c, type):
            continue

        if not issubclass(c, CouchbaseTestCase):
            continue

        globals()[c.__name__] = c

if __name__ == "__main__":
    for m in testclases:
        print(m.__name__)
