import copy
import pyrsistent
import json
from couchbase_core.transcodable import Transcodable


class JSONDocument(Transcodable):
    record = None  # type: pyrsistent.PMap

    def __init__(self, parent=None,
                 **kwargs):
        self.record = parent.record if parent else pyrsistent.pmap()
        for k, v in kwargs.items():
            self.record = self.record.set(k, v)

    def put(self, key, value):
        # type: (str, JSON) -> JSONDocument
        result=JSONDocument(parent=self)
        result.record=result.record.set(key,value)
        return result

    def encode_canonical(self):
        return pyrsistent.thaw(self.record)

    @classmethod
    def decode_canonical(cls, input):
        result = cls()
        result.record = pyrsistent.freeze(input)
        return result

    def __eq__(self,
               other):
        return type(other)==type(self) and other.record==self.record

    def __hash__(self):
        return hash(self.record)