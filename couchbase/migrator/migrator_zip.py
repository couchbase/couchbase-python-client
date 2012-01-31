# source *.json files from a zip file into destination

sources=[{'type':'zip','class':'ZipReader','example':'zip://<zipfile>'}]
destinations=[{'type':'zip','class':'ZipWriter','example':'zip://<zipfile>'}]

import os
import json
import zipfile
import tempfile
import shutil

import migrator
from migrator_dir import DirReader, DirWriter

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
        self.zipfile = zipfile.ZipFile(os.path.expanduser(destination), "w", zipfile.ZIP_DEFLATED)
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
