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

# source *.json files in a directory into destination

# source:
# recurses through subdirectories and reads .json files
# if the .json has an _id field it will use that, otherwise it will use the filename
# if the directory contains an _id file, the directory itself will be considered a document and all files (except *.json) will be considered json data

# destination:
# writes out items that have _id: _design/* to <dir>/design_docs
# writes out all other items to <dir>/docs

sources=[{'type':'dir','class':'DirReader','example':'dir://<directory>'}]
destinations=[{'type':'dir','class':'DirWriter','example':'dir://<directory>'}]

import os
import json

import migrator

class DirReader(migrator.Reader):
    def __init__(self, source):
        if source[0:2] == "//":
            source = source[2:]
        self.dir = os.path.expanduser(source)
        self.files = self._get_filenames()

    def __iter__(self):
        return self

    def _get_filenames(self):
        filenames = []
        for root, subFolders, files in os.walk(self.dir):
            for filename in files:
                if filename.endswith(".json") or filename == "_id":
                    filepath = os.path.join(root, filename)
                    filenames.append(filepath)
        return filenames

    def _get_nonjson_filenames(self, base):
        filenames = []
        for root, subFolders, files in os.walk(base):
            for filename in files:
                if not filename.endswith(".json") and not filename == "_id":
                    filepath = os.path.join(root, filename)
                    filenames.append(filepath)
        return filenames

    def next(self):
        try:
            filename = self.files.pop(0)
            path = os.path.join(self.dir, filename)
        except IndexError:
            raise StopIteration()

        if os.path.basename(filename) == "_id":
            # entire directory (except *.json) is a document
            # <dir>/views is converted to <id>/_views
            doc_basepath = os.path.dirname(filename)
            with open(filename, 'r') as f:
                id = f.read().strip('\n\r,')
            json_data = {}
            for f in self._get_nonjson_filenames(os.path.dirname(filename)):
                f_relative_path = f[len(doc_basepath)+1:]
                if os.path.isfile(os.path.join(doc_basepath,f_relative_path.split(os.path.sep)[0])):
                    # its a plain file
                    with open (f) as item:
                        json_data[f_relative_path.split(os.path.sep)[0]] = item.read().strip('\n\r,')
                else:
                    # its a subdirectory
                    json_cur = json_data
                    for index in f_relative_path.split(os.path.sep)[:-1]:
                        if not index in json_cur:
                            json_cur[index] = {}
                        json_cur = json_cur[index]

                    index = os.path.splitext(f_relative_path.split(os.path.sep)[-1])[0]
                    with open(f) as item:
                        json_cur[index] = item.read().strip('\n\r,')
        else:
            with open(filename, 'r') as f:
                json_data = json.loads(f.read().strip('\n\r,'))
                if "_id" in json_data:
                    id = json_data["_id"]
                else:
                    id = os.path.splitext(os.path.basename(filename))[0]

        record = {'id':id}
        record['value'] = dict((k,v) for (k,v) in json_data.iteritems() if not k.startswith('_'))
        return record


class DirWriter(migrator.Writer):
    def __init__(self, destination):
        if destination[0:2] == "//":
            destination = destination[2:]
        self.dir = os.path.expanduser(destination)
        try:
            os.makedirs(os.path.join(destination,"docs"))
            os.makedirs(os.path.join(destination,"design_docs"))
        except OSError as e:
            pass

    def write(self, record):
        if os.path.sep in record["id"]:
            record["value"]["_id"] = record["id"]
        if record["id"].startswith("_design/"):
            filename = record["id"][len("_design/"):].replace(os.path.sep, "_")
            path = os.path.join(self.dir, "design_docs", filename) + ".json"
        else:
            filename = record["id"].replace(os.path.sep, "_")
            path = os.path.join(self.dir, "docs", filename) + ".json"
        f = open(path, "wb")
        f.write(json.dumps(record["value"]) + '\n')
