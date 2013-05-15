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

    ret = "{cls}<".format(cls = self.__class__.__name__)
    ret += "RC=0x{rc:x}{errdesc}".format(rc=self.rc, errdesc=errdesc)

    if hasattr(self, 'key'):
        ret += ", Key={0}".format(self.key)

    if hasattr(self, 'value'):
        ret += ", Value={0}".format(repr(self.value))

    if hasattr(self, 'cas'):
        ret += ", CAS=0x{cas:x}".format(cas=self.cas)

    if hasattr(self, 'flags'):
        ret += ", Flags=0x{flags:x}".format(flags=self.flags)

    ret += ">"
    return ret

def _json_encode_wrapper(*args):
    return json.dumps(*args, ensure_ascii=False)

C._init_helpers(
                result_reprfunc = _result__repr__,
                fmt_utf8_flags = C.FMT_UTF8,
                fmt_bytes_flags = C.FMT_BYTES,
                pickle_encode = pickle.dumps,
                pickle_decode = pickle.loads,
                json_encode = _json_encode_wrapper,
                json_decode = json.loads,
                lcb_errno_map = E._LCB_ERRNO_MAP,
                misc_errno_map = E._EXCTYPE_MAP,
                default_exception = E.CouchbaseError)
