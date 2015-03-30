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
from __future__ import print_function
import os
import os.path

imps = []
testmods = []
testclasses = []

for name in os.listdir(os.path.join(os.path.dirname(__file__), 'cases')):
    if name.startswith('__init__'):
        continue

    name, ext = os.path.splitext(name)
    if ext.lower() != '.py':
        continue

    imps.append(name)

def _get_packages():
    """
    Returns a dictionary of { name: module_object } for all cases
    """
    ret = {}
    for modname in imps:
        # print(repr(modname))

        module = __import__('couchbase.tests.cases.'+modname,
                            fromlist=('couchbase', 'tests', 'cases'))
        ret[modname] = module
    return ret

def _get_classes(modules):
    """
    Returns an extracted dictionary of { name: test_class } as combined
    from all the modules provided
    """
    ret = {}

    for module in modules:
        for attrname in dir(module):
            attrobj = getattr(module, attrname)

            if not isinstance(attrobj, type):
                continue

            from couchbase.tests.base import CouchbaseTestCase
            if not issubclass(attrobj, CouchbaseTestCase):
                continue

            ret[attrname] = attrobj

    return ret


def get_configured_classes(implconfig, implstr=None, skiplist=None):
    """
    returns a tuple of (module_dict, testcase_dict)
    :param implstr: A unique string to be appended to each test case
    :param implconfig: An ApiConfigurationMixin to use as the mixin for
    the test class.
    """
    d_mods = _get_packages()
    d_cases = _get_classes(d_mods.values())
    ret = {}

    if not implstr:
        implstr = "_" + implconfig.factory.__name__

    if not skiplist:
        skiplist = []

    for name, case in d_cases.items():
        if name in skiplist:
            continue

        cls = type(name+implstr, (case, implconfig), {})
        ret[name+implstr] = cls

    return ret

if __name__ == "__main__":
    mods, classes = get_all()
    for cls in classes.values():
        print(cls.__name__)
