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

from couchbase.tests.base import SkipTest
import sys

vi = sys.version_info
if vi[0] == 3:
    from unittest.case import SkipTest
    raise SkipTest("Twisted support unavailable on Python3")

try:
    from twisted.trial.unittest import TestCase
except:
    raise SkipTest("Twisted not found")

