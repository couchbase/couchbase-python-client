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

try:
    import urllib.parse as ulp
    from urllib.request import urlopen
    from urllib.parse import parse_qs
except ImportError:
    import urllib as ulp
    from urllib2 import urlopen
    from urlparse import parse_qs

try:
    long = long
except NameError:
    long = int

try:
    xrange = xrange
except NameError:
    xrange = range

try:
    basestring = basestring
except NameError:
    basestring = str

try:
    unicode = unicode
except NameError:
    unicode = str
