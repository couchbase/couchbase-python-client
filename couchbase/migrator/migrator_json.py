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

sources=[{'type':'json','class':'JSONReader','example':'json://<filename>'}]
destinations=[{'type':'json','class':'JSONWriter','example':'json://<filename>'}]

import json

import migrator

class JSONReader(migrator.Reader):
    def __init__(self, source):
        if source[0:2] == "//":
            source = source[2:]
        self.reader = open(source, 'rb')

    def __iter__(self):
        return self

    def next(self):
        data = self.reader.next()
        if data:
            try:
                json_data = json.loads(data.strip('\n\r,'))
            except ValueError:
                raise StopIteration()
            record = {'id':json_data['id']}
            record['value'] = dict((k,v) for (k,v) in json_data['value'].iteritems() if not k.startswith('_'))
            return record
        else:
            raise StopIteration()


class JSONWriter(migrator.Writer):
    def __init__(self, destination):
        if destination[0:2] == "//":
            destination = destination[2:]
        self.file = open(destination, 'w')

    def write(self, record):
        self.file.write(json.dumps(record) + '\n')
