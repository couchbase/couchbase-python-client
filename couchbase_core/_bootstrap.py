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

import couchbase_core.exceptions as E
import couchbase_core._libcouchbase as C
from couchbase_core.items import ItemCollection, ItemOptionDict, ItemSequence
from couchbase_core.result import SubdocResult
from couchbase_core.subdocument import MultiValue
from .transcodable import Transcodable


def _result__repr__(self):
    """
    This is used as the `__repr__` function for the :class:`Result`
    """

    details = []
    flags = self.__class__._fldprops

    rcstr = "rc=0x{0:X}".format(self.rc)
    if self.rc != 0:
        rcstr += "[{0}]".format(self.errstr)

    details.append(rcstr)

    if flags & C.PYCBC_RESFLD_KEY and hasattr(self, 'key'):
        details.append("key={0}".format(repr(self.key)))

    if flags & C.PYCBC_RESFLD_VALUE and hasattr(self, 'value'):
        details.append("value={0}".format(repr(self.value)))

    if flags & C.PYCBC_RESFLD_CAS and hasattr(self, 'cas'):
        details.append("cas=0x{cas:x}".format(cas=self.cas))

    if flags & C.PYCBC_RESFLD_CAS and hasattr(self, 'flags'):
        details.append("flags=0x{flags:x}".format(flags=self.flags))

    if flags & C.PYCBC_RESFLD_HTCODE and hasattr(self, "http_status"):
        details.append("http_status={0}".format(self.http_status))

    if flags & C.PYCBC_RESFLD_URL and hasattr(self, "url"):
        details.append("url={0}".format(self.url))

    if hasattr(self, "tracing_context"):
        details.append("tracing_context={0}".format(self.tracing_context))

    if hasattr(self, "tracing_output"):
        details.append("tracing_output={0}".format(self.tracing_output))

    if hasattr(self, '_pycbc_repr_extra'):
        details += self._pycbc_repr_extra()

    ret = "{0}<{1}>".format(self.__class__.__name__, ', '.join(details))
    return ret


def _observeinfo__repr__(self):
    constants = ('OBS_PERSISTED',
                 'OBS_FOUND',
                 'OBS_NOTFOUND',
                 'OBS_LOGICALLY_DELETED')


    flag_str = ''
    for c in constants:
        if self.flags == getattr(C, c):
            flag_str = c
            break

    fmstr = ("{cls}<Status=[{status_s} (0x{flags:X})], "
             "Master={is_master}, "
             "CAS=0x{cas:X}>")
    ret = fmstr.format(cls=self.__class__.__name__,
                       status_s=flag_str,
                       flags=self.flags,
                       is_master=bool(self.from_master),
                       cas=self.cas)
    return ret


def _json_encode_wrapper(*args):
    encodable = []
    for arg in args:
        if isinstance(arg, Transcodable) and hasattr(arg, 'encode_canonical'):
            encodable.append(arg.encode_canonical())
        else:
            encodable.append(arg)

    return json.dumps(*encodable, ensure_ascii=False, separators=(',', ':'))


class FMT_AUTO_object_not_a_number(object):
    pass

# TODO: Make this more readable and have PEP8 ignore it.
_FMT_AUTO = FMT_AUTO_object_not_a_number()


MAX_URI_LENGTH = 2048


def _view_path_helper(options):
    # Assume options are already encoded!
    if not options:
        return '', ''

    post_body = ''
    encoded = options.encoded

    if len(encoded) > MAX_URI_LENGTH:
        encoded, post_body = options._long_query_encoded

    return encoded, post_body


def run_init(m):
    m._init_helpers(result_reprfunc=_result__repr__,
                    fmt_utf8_flags=C.FMT_UTF8,
                    fmt_bytes_flags=C.FMT_BYTES,
                    fmt_json_flags=C.FMT_JSON,
                    fmt_pickle_flags=C.FMT_PICKLE,
                    pickle_encode=pickle.dumps,
                    pickle_decode=pickle.loads,
                    json_encode=_json_encode_wrapper,
                    json_decode=json.loads,
                    lcb_errno_map=E._LCB_ERRNO_MAP,
                    misc_errno_map=E._EXCTYPE_MAP,
                    default_exception=E.CouchbaseError,
                    obsinfo_reprfunc=_observeinfo__repr__,
                    itmcoll_base_type=ItemCollection,
                    itmopts_dict_type=ItemOptionDict,
                    itmopts_seq_type=ItemSequence,
                    fmt_auto=_FMT_AUTO,
                    view_path_helper=_view_path_helper,
                    sd_result_type=SubdocResult,
                    sd_multival_type=MultiValue)

run_init(C)

C.FMT_AUTO = _FMT_AUTO


def describe_lcb_api():
    return "PYCBC_LCB_API {}, LCB API version {}".format(pycbc_lcb_api_str(), C.LCB_VERSION_STRING)


def pycbc_lcb_api_str():
    return '0x{0:0{1}X}'.format(C.PYCBC_LCB_API, 6)