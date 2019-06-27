#
# Copyright 2019, Couchbase, Inc.
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
import sys

from setuptools import Extension
import platform
import warnings

from cbuild_config import couchbase_core, install_headers, CBuildCommon


def gen_cmodule(extoptions):
    if platform.python_implementation() != 'PyPy':
        print("sources are {}".format(extoptions['sources']))
        module = Extension(str(couchbase_core+'._libcouchbase'), **extoptions)
    else:
        warnings.warn('The C extension libary does not work on PyPy. '
                      'You should install the couchbase_ffi module. Installation of this '
                      'module will continue but will be unusable without couchbase_ffi')
        module = None
    return module


def gen_distutils_build(extoptions,pkgdata):
    e_mods = [gen_cmodule(extoptions)]
    CBuildCommon.setup_build_info(extoptions,pkgdata)
    cmdclass = {'install_headers': install_headers, 'build_ext': CBuildCommon}
    return e_mods, cmdclass


