import copy
import pyrsistent
import json
from couchbase_core.transcodable import Transcodable


class JSONDocument(Transcodable):
    record = None  # type: pyrsistent.PRecord

    def __init__(self, parent=None,
                 **kwargs):
        self.record = parent.record if parent else pyrsistent.PRecord()
        for k, v in kwargs.items():
            self.record = self.record.set(k, v)

    def put(self, key, value):
        # type: (str, JSON) -> JSONDocument
        return JSONDocument(parent=self, **{key: value})

    def encode_canonical(self):
        return pyrsistent.thaw(self.record)

    @classmethod
    def decode_canonical(cls, input):
        result = cls()
        result.record = pyrsistent.freeze(input)
        return result
