from couchbase_core.subdocument import Spec
from .options import Seconds, FiniteDuration
from couchbase_core.transcodable import Transcodable
from couchbase_core._libcouchbase import Result as SDK2Result, Result
from typing import *
from boltons.funcutils import wraps


Proxy_T = TypeVar('Proxy_T')

try:
    from abc import abstractmethod
except:
    pass

from couchbase_core.result import MultiResult, SubdocResult


def canonical_sdresult(content):
    sdresult = content  # type: SubdocResult
    result = {}
    cursor = iter(sdresult)
    for index in range(0, sdresult.result_count):
        spec = sdresult._specs[index]  # type: Spec
        result[spec[1]] = next(cursor)
    return result


def extract_value(content, decode_canonical):
    if isinstance(content, MultiResult):
        return {k: decode_canonical(content[k].value) for k, v, in content}
    elif isinstance(content, SubdocResult):
        return decode_canonical(canonical_sdresult(content))
    return decode_canonical(content.value)


def get_decoder(item  # type: Type[Union[Transcodable,Any]]
                ):
    return getattr(item, 'decode_canonical', None) if issubclass(item, Transcodable) else item


class ContentProxy(object):
    def __init__(self, content):
        self.content = content

    def __getitem__(self,
                    item  # type: Type[Proxy_T]
                    ):
        # type: (...)->Union[Proxy_T,Mapping[str,Proxy_T]]
        return extract_value(self.content, get_decoder(item))


class ContentProxySubdoc(object):
    def __init__(self, content):
        self.content=content

    def index_proxy(self, item, index):
        return get_decoder(item)(self.content[index])

    def __getitem__(self,
                    item  # type: Type[Proxy_T]
                    ):
        # type: (...)->Callable[[int],Union[Proxy_T,Mapping[str,Proxy_T]]]
        return lambda index: self.index_proxy(item, index)


class IResult(object):
    def __init__(self,
                 cas,  # type: int
                 error=None  # type: Optional[int]
                 ):
        self._cas = cas
        self._error = error

    @property
    def cas(self):
        # type: ()->int
        return self._cas

    @property
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


class LookupInResult(IResult):
    def __init__(self,
                 content,  # type: SDK2Result
                 *args,  # type: Any
                 **kwargs  # type: Any
                 ):
        # type: (...) ->None
        """
        LookupInResult is the return type for lookup_in operations.
        Constructed internally by the API.
        """
        self._content = content  # type: SDK2Result
        self.dict = kwargs

    @property
    def content_as(self):
        # type: (...)->ContentProxySubdoc
        return ContentProxySubdoc(self._content)

    def exists(self,
               index  # type: int
               ):
        return len(canonical_sdresult(self._content))>index


class MutationResult(IResult):
    def __init__(self,
                 cas,  # type: int
                 mutation_token=None  # type: MutationToken
                 ):
        super(MutationResult, self).__init__(cas)
        self.mutationToken = mutation_token

    def mutation_token(self):
        # type: () -> MutationToken
        return self.mutationToken


class MutateInResult(MutationResult):
    def __init__(self,
                 content,  # type: SDK2Result
                 **options  # type: Any
                 ):
        # type: (...) ->None
        """
        MutateInResult is the return type for mutate_in operations.
        Constructed internally by the API.
        """
        self._content = content  # type: SDK2Result
        self.dict = options

    def content_as(self):
        # type: (...)->ContentProxy
        return ContentProxy(self._content)


class GetResult(IGetResult):
    def __init__(self,
                 content,  # type: SDK2Result
                 *args,  # type: Any
                 **kwargs  # type: Any
                 ):
        # type: (...) ->None
        """
        GetResult is the return type for full read operations.
        Constructed internally by the API.
        """
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
        return extract_value(self._content, lambda x:x)


ResultPrecursor = NamedTuple('ResultPrecursor', [('orig_result',SDK2Result), ('orig_options', Mapping[str,Any])])


def get_result(x,  # type: SDK2Result
               options=None  # type: Dict[str,Any]
               ):
    # type: (...)->GetResult
    options = options or {}

    return GetResult(x, cas=getattr(x, 'cas'), expiry=options.pop('timeout', None), id=x.key)


def get_result_wrapper(func  # type: Callable[[Any], ResultPrecursor]
                       ):
    # type: (...)->Callable[[Any], GetResult]
    @wraps(func)
    def wrapped(*args, **kwargs):
        x, options = func(*args,**kwargs)
        return get_result(x, options)
    wrapped.__name__=func.__name__
    wrapped.__doc__=func.__name__
    return wrapped


class MutationToken(object):
    def __init__(self, sequenceNumber,  # type: int
                 vbucketId,  # type: int
                 vbucketUUID  # type: int
                 ):
        self.sequenceNumber = sequenceNumber
        self.vbucketId = vbucketId
        self.vbucketUUID = vbucketUUID

    def partition_id(self):
        # type: (...)->int
        pass

    def partition_uuid(self):
        # type: (...)->int
        pass

    def sequence_number(self):
        # type: (...)->int
        pass

    def bucket_name(self):
        # type: (...)->str
        pass


class SDK2MutationToken(MutationToken):
    def __init__(self, token):
        token=token or (None,None,None)
        super(SDK2MutationToken,self).__init__(token[2],token[0],token[1])


def get_mutation_result(result  # type: ResultPrecursor
                        ):
    # type (...)->MutationResult
    return MutationResult(result.orig_result.cas, SDK2MutationToken(result.orig_result._mutinfo) if hasattr(result.orig_result, '_mutinfo') else None)


def _wrap_in_mutation_result(func  # type: Callable[[Any,...],SDK2Result]
                             ):
    # type: (...)->Callable[[Any,...],MutationResult]
    @wraps(func)
    def mutated(*args, **kwargs):
        result = func(*args, **kwargs)
        return get_mutation_result(result)

    mutated.__name__=func.__name__
    mutated.__doc__=func.__doc__
    return mutated