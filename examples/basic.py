from couchbase import Couchbase, FMT_PICKLE
from couchbase.exceptions import KeyExistsError

# Connect to the default bucket on local host
cb = Couchbase.connect('127.0.0.1', 8091, '', '', 'default')

# If you want to store the Python objects pickled and not as JSON
#cb.default_format = FMT_PICKLE

# Store a document
cas = cb.set('first', {'hello': 'world'})
print(cas)

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
    item, flags, correct_cas = cb.get('first', extended=True)
    # Set it again, this time with the correct CAS value
    cas = cb.set('first', {'hello': 'world', 'additional': True},
                 cas=correct_cas)
    print(cas)

# Delete the document only if the CAS value matches (it would also
# work without a cas value)
cb.delete('first', cas=cas)

# Make sure the document really got deleted
assert cb.get('first', quiet=True) is None
