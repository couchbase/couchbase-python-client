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

# source *.json files from a zip file into destination

import os
import zipfile
import tempfile
import shutil

from couchbase.migrator.migrator_dir import DirReader, DirWriter

sources = [{'type':'zip', 'class':'ZipReader', 'example':'zip://<zipfile>'}]
destinations = [{'type':'zip', 'class':'ZipWriter',
                 'example':'zip://<zipfile>'}]


class ZipReader(DirReader):
    def __init__(self, source):
        if source[0:2] == "//":
            source = source[2:]
        self.zipfile = zipfile.ZipFile(os.path.expanduser(source), "r")
        self.tempdir = tempfile.mkdtemp()

        self.zipfile.extractall(self.tempdir)

        DirReader.__init__(self, self.tempdir)

    def close(self):
        shutil.rmtree(self.tempdir)
        self.zipfile.close()


class ZipWriter(DirWriter):
    def __init__(self, destination):
        if destination[0:2] == "//":
            destination = destination[2:]
        self.zipfile = zipfile.ZipFile(os.path.expanduser(destination), "w",
                                       zipfile.ZIP_DEFLATED)
        self.dirname = os.path.splitext(os.path.basename(destination))[0]
        self.tempdir = tempfile.mkdtemp()

        DirWriter.__init__(self, self.tempdir)

    def close(self):
        for root, dirs, files in os.walk(self.tempdir):
            f_relative_path = os.path.abspath(root)[len(self.tempdir) + 1:]
            for f in files:
                f_path = os.path.join(root, f)
                archive_path = os.path.join(self.dirname, f_relative_path, f)
                self.zipfile.write(f_path, archive_path)

        shutil.rmtree(self.tempdir)
        self.zipfile.close()
