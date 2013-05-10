#!/usr/bin/env python

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
    rv = cb.set('first', {'hello': 'world', 'additional': True},
                 cas=correct_cas)
    print(rv)

# Delete the document only if the CAS value matches (it would also
# work without a cas value)
cb.delete('first', cas=rv.cas)

# Make sure the document really got deleted
assert cb.get('first', quiet=True).success is False
