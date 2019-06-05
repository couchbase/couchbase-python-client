#
# Copyright 2015, Couchbase, Inc.
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

import unittest
from couchbase_tests.base import SkipTest


try:
    from couchbase_version import VersionInfo
except ImportError:
    VersionInfo = None


class VersionInfoTest(unittest.TestCase):
    def setUp(self):
        super(VersionInfoTest, self).setUp()
        if VersionInfo is None:
            raise SkipTest("Don't have couchbase_version")

    def test_version_parse(self):
        info = VersionInfo('2.0.1-99-gblahblah')
        self.assertEqual('2.0.1.dev99+gblahblah', info.package_version)

        info = VersionInfo('2.0.0-dp3-34-gff')
        self.assertEqual('2.0.0a3.dev34+gff', info.package_version)

        info = VersionInfo('2.0.0-0-gdeadbeef')
        self.assertEqual('2.0.0', info.package_version)

        info = VersionInfo('2.0.0-beta-9-gff')
        self.assertEqual('2.0.0b0.dev9+gff', info.package_version)

        info = VersionInfo('2.0.0-beta3-0-gff')
        self.assertEqual('2.0.0b3', info.package_version)


if __name__ == '__main__':
    import unittest
    unittest.main()