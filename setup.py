#
# Copyright 2012, Couchbase, Inc.
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


import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from version import get_git_version

here = os.path.dirname(os.path.abspath(__file__))
long_description = open(os.path.join(here, 'README.md')).read()

setup(
    name="couchbase",
    version=get_git_version(),
    description="Couchbase Python SDK",
    author="Couchbase, Inc.",
    author_email="info@couchbase.com",
    packages=["couchbase", "couchbase/utils", "couchbase/migrator"],
    install_requires=["requests==0.14.0", "unittest2", "uuid"],
    setup_requires=["nose>=1.0"],
    tests_require=["nose-testconfig"],
    url="http://www.couchbase.com/develop/python/next",
    license="LICENSE.txt",
    keywords=["encoding", "i18n", "xml"],
    classifiers=["Programming Language :: Python",
                 "Programming Language :: Python :: 2.6",
                 "Development Status :: 4 - Beta",
                 "Environment :: Other Environment",
                 "Intended Audience :: Developers",
                 "License :: OSI Approved :: Apache Software License",
                 "Operating System :: OS Independent",
                 "Topic :: Software Development :: Libraries :: " +
                 "Python Modules",
                 ],
    long_description=long_description,
)
