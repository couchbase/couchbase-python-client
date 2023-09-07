#  Copyright 2016-2023. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import platform
import sys

from setuptools import find_packages, setup

sys.path.append('.')
import couchbase_version  # nopep8 # isort:skip # noqa: E402
from pycbc_build_setup import (BuildCommand,  # nopep8 # isort:skip # noqa: E402
                               CMakeBuildExt,
                               CMakeExtension)

try:
    couchbase_version.gen_version()
except couchbase_version.CantInvokeGit:
    pass

PYCBC_README = os.path.join(os.path.dirname(__file__), 'README.md')
PYCBC_VERSION = couchbase_version.get_version()


package_data = {}
# some more Windows tomfoolery...
if platform.system() == 'Windows':
    package_data = {'couchbase': ['pycbc_core.pyd']}

print(f'Python SDK version: {PYCBC_VERSION}')

setup(name='couchbase',
      version=PYCBC_VERSION,
      ext_modules=[CMakeExtension('couchbase.pycbc_core')],
      cmdclass={'build': BuildCommand, 'build_ext': CMakeBuildExt},
      python_requires='>=3.7',
      packages=find_packages(
          include=['acouchbase', 'couchbase', 'txcouchbase', 'couchbase.*', 'acouchbase.*', 'txcouchbase.*'],
          exclude=['acouchbase.tests', 'couchbase.tests', 'txcouchbase.tests']),
      package_data=package_data,
      url="https://github.com/couchbase/couchbase-python-client",
      author="Couchbase, Inc.",
      author_email="PythonPackage@couchbase.com",
      license="Apache License 2.0",
      description="Python Client for Couchbase",
      long_description=open(PYCBC_README, "r").read(),
      long_description_content_type='text/markdown',
      keywords=["couchbase", "nosql", "pycouchbase", "libcouchbase"],
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: Implementation :: CPython",
          "Topic :: Database",
          "Topic :: Software Development :: Libraries",
          "Topic :: Software Development :: Libraries :: Python Modules"],
      )
