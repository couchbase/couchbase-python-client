from typing import *

from couchbase_core.subdocument import Spec
from .options import Seconds
from couchbase_core.transcodable import Transcodable
from couchbase_core._libcouchbase import Result as SDK2Result


Proxy_T = TypeVar('Proxy_T')

try:
    from abc import abstractmethod
except:
    pass

from couchbase_core.result import MultiResult, SubdocResult


class ContentProxy:
    def __init__(self, content):
        self.content = content

    def canonical_sdresult(self):
        sdresult=self.content  # type: SubdocResult
        result = {}
        cursor=iter(sdresult)
        for index in range(0,sdresult.result_count):
            spec = sdresult._specs[index]  # type: Spec
            result[spec[1]]=next(cursor)
        return result

    def __getitem__(self,
                    item  # type: Type[Proxy_T]
                    ):
        # type: (...)->Proxy_T
        decode_canonical = getattr(item, 'decode_canonical', None) if issubclass(item, Transcodable) else item
        if isinstance(self.content, MultiResult):
            return {k: decode_canonical(self.content[k].value) for k,v, in self.content}
        elif isinstance(self.content, SubdocResult):
            return decode_canonical(self.canonical_sdresult())
        return decode_canonical(self.content.value)


class IResult(object):
    def __init__(self,
                 cas,  # type: int
                 error=None  # type: Optional[int]
                 ):
        self.cas = cas
        self.error = error

    def cas(self):
        # type: ()->int
        return self.cas

    def error(self):
        # type: ()->int
        return self.error

    def success(self):
        # type: ()->bool
        return not self.error


class IGetResult(IResult):
    @property
    @abstractmethod
    def id(self):
        # type: ()->str
        pass

    @property
    @abstractmethod
    def expiry(self):
        # type: ()->FiniteDuration
        pass


class GetResult(IGetResult):
    def __init__(self,
                 content,  # type: SDK2Result
                 *args,  # type: Any
                 **kwargs  # type: Any
                 ):
        # type: (...) ->None
        self._content = content  # type: SDK2Result
        self.dict = kwargs

    def content_as_array(self):
        # type: (...) ->List
        return list(self.content)

    @property
    def content_as(self):
        # type: (...)->ContentProxy
        return ContentProxy(self._content)

    def __getitem__(self, t):
        return

    @property
    def id(self):
        # type: () -> str
        return self.dict['id']

    @property
    def cas(self):
        # type: () -> int
        return self.dict['cas']

    @property
    def expiry(self):
        # type: () -> Seconds
        return Seconds(self.dict['expiry'])

    @property
    def content(self):
        # type: () -> SDK2Result
        return self._content


def get_result(x,  # type: SDK2Result
               options=None):
    options = options or {}
    return GetResult(x, cas=x.cas, expiry=options.pop('timeout', None), id=x.key)


def get_result_wrapper(func):
    def wrapped(*args, **kwargs):
        x, options = func(*args,**kwargs)
        return get_result(x, options)
    return wrapped

