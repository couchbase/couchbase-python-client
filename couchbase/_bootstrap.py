"""
This module contains the core functionality of '_libcouchbase'. In short,
this contains the convergence between the C module and code written in Python.

While the _libcouchbase module should never be used directly, in the off chance
that this does happen, ensure this module is loaded as well before anything is
done, otherwise Bad Things May Happen.

Additionally, this
module contains python functions used exclusively from C. They are here
because it was quicker to write them in Python than it was in C. Do not touch
this file at all. You have been warned
"""
import json
import pickle

import couchbase.exceptions as E
import couchbase._libcouchbase as C

def _result__repr__(self):
    """
    This is used as the `__repr__` function for the :class:`Result`
    """
    errdesc = ""

    if self.rc != 0:
        errdesc = "[{0}]".format(self.errstr)

    return ("{cls}<"
            "RC=0x{rc:x}{errdesc}, "
            "Value={val}, "
            "Flags=0x{flags:x}, "
            "CAS=0x{cas:x}"
            ">").format(rc = self.rc,
                        val = self.value,
                        flags = self.flags,
                        cas = self.cas,
                        errdesc = errdesc,
                        cls=self.__class__.__name__)

C._init_helpers(
                result_reprfunc = _result__repr__,
                fmt_utf8_flags = C.FMT_UTF8,
                fmt_bytes_flags = C.FMT_BYTES,
                pickle_encode = pickle.dumps,
                pickle_decode = pickle.loads,
                json_encode = json.dumps,
                json_decode = json.loads,
                lcb_errno_map = E._LCB_ERRNO_MAP,
                misc_errno_map = E._EXCTYPE_MAP,
                default_exception = E.CouchbaseError)
