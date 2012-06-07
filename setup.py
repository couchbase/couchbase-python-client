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

from distutils.core import setup
import subprocess

def get_version():
    try:
        p = subprocess.Popen('git describe', stdout=subprocess.PIPE, shell=True)
        version = p.communicate()[0].strip()
    except:
        version = ''
    return version


setup(
    name = "couchbase-python",
    version = get_version(),
    description = "Couchbase Python SDK",
    author = "Couchbase Inc",
    author_email = "info@couchbase.com",
    packages = ["couchbase", "httplib2", "couchbase/utils", "couchbase/migrator"],
    url = "http://couchbase.org/",
    download_url = "http://.../pysdk.tar.gz",
    license = "LICENSE.txt",
    keywords = ["encoding", "i18n", "xml"],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    long_description = open('README.md').read(),
)
