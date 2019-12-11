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

# This module contains various mappings for modules which have had
# their names changed across Python major versions
import sys

V = sys.version_info[0]

if V == 3:
    import urllib.parse as ulp
    from urllib.request import urlopen
    from urllib.parse import parse_qs
    izip = zip
else:
    import urllib as ulp
    from urllib2 import urlopen
    from urlparse import parse_qs
    from itertools import izip

long = long if V == 2 else int
xrange = xrange if V == 2 else range
basestring = basestring if V == 2 else str
unicode = unicode if V == 2 else str

if V == 2:
    exec("def PyErr_Restore(cls, obj, bt): raise cls, obj, bt\n")
else:
    def PyErr_Restore(cls, obj, bt):
        raise obj.with_traceback(bt)

if V == 2:
    def single_dict_key(d):
        return d.keys()[0]
else:
    def single_dict_key(d):
        for k in d.keys():
            return k

ANY_STR = (basestring, str)

try:
    from six import with_metaclass
except:
    from future.utils import with_metaclass

try:
    from typing import *
except:
    pass

try:
    from typing_extensions import *
except:
    pass

from six import raise_from