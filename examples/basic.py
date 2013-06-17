#!/usr/bin/env python
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

from couchbase import Couchbase, FMT_PICKLE
from couchbase.exceptions import KeyExistsError


# Connect to the default bucket on local host
cb = Couchbase.connect(host='127.0.0.1', bucket='default')

# If you want to store the Python objects pickled and not as JSON
#cb.default_format = FMT_PICKLE

# Store a document
rv = cb.set('first', {'hello': 'world'})
cas = rv.cas
print(rv)

# Get the document
item = cb.get('first')
print(item)

# Overwrite the existing document only if the CAS value matched
try:
    # An exception will be raised if the CAS doesn't match
    wrong_cas = cas + 123
    cb.set('first', {'hello': 'world', 'additional': True}, cas=wrong_cas)
except KeyExistsError:
    # Get the correct current CAS value
    rv = cb.get('first')
    item, flags, correct_cas = rv.value, rv.flags, rv.cas
    # Set it again, this time with the correct CAS value
    rv = cb.set('first',
                {'hello': 'world', 'additional': True},
                cas=correct_cas)
    print(rv)

# Delete the document only if the CAS value matches (it would also
# work without a cas value)
cb.delete('first', cas=rv.cas)

# Make sure the document really got deleted
assert cb.get('first', quiet=True).success is False
