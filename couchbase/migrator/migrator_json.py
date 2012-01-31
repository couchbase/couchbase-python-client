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
