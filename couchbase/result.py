from couchbase_core.subdocument import Spec
from couchbase_core.supportability import internal
from .options import timedelta, forward_args
from couchbase_core.transcodable import Transcodable
from couchbase_core._libcouchbase import Result as CoreResult
from couchbase_core.result import MultiResult, SubdocResult
from typing import *
from boltons.funcutils import wraps
from couchbase_core import abstractmethod, IterableWrapper
from couchbase_core.result import AsyncResult
from couchbase_core._pyport import Protocol
from couchbase_core.views.iterator import View as CoreView


Proxy_T = TypeVar('Proxy_T')


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
    """
    Used to provide access to Result content via Result.content_as[type]
    """
    @internal
    def __init__(self, content):
        self.content = content

    def __getitem__(self,
                    item  # type: Type[Proxy_T]
                    ):
        # type: (...) -> Union[Proxy_T, Mapping[str,Proxy_T]]
        """

        :param item: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return extract_value(self.content, get_decoder(item))


class ContentProxySubdoc(object):
    """
    Used to provide access to Result content via Result.content_as[type]
    """
    @internal
    def __init__(self, content):
        self.content=content

    def index_proxy(self, item, index):
        return get_decoder(item)(self.content[index])

    def __getitem__(self,
                    item  # type: Type[Proxy_T]
                    ):
        # type: (...) -> Callable[[int],Union[Proxy_T,Mapping[str,Proxy_T]]]
        """
        Returns a proxy for an array of subdoc results cast to the given type

        :param item: type to cast the array elements to
        :return: the proxy, which is callable with an index to extract from the array and cast
        """
        return lambda index: self.index_proxy(item, index)


class ResultProtocol(Protocol):
    """
    This is the base protocol for all Result Objects
    """
    @property
    @abstractmethod
    def cas(self):
        # type: () -> int
        raise NotImplementedError()

    @property
    @abstractmethod
    def error(self):
        # type: () -> int
        raise NotImplementedError()

    @property
    @abstractmethod
    def success(self):
        # type: () -> bool
        raise NotImplementedError()


class Result(ResultProtocol):
    @internal
    def __init__(self,
                 cas,  # type: int
                 error=None  # type: Optional[int]
                 ):
        """
        This is the base implementation for most (but not all) :class:`~.ResultProtocol` objects.

        :param int cas: CAS value
        :param Optional[int] error: error code if applicable
        """
        self._cas = cas
        self._error = error

    @property
    def cas(self):
        # type: () -> int
        """
        The CAS value

        :return: the CAS value
        """
        return self._cas

    @property
    def error(self):
        # type: () -> int
        return self._error

    def success(self):
        # type: () -> bool
        return not self.error


class LookupInResult(Result):
    @internal
    def __init__(self,
                 content,  # type: CoreResult
                 **kwargs  # type: Any
                 ):
        # type: (...) -> None
        """
        LookupInResult is the return type for lookup_in operations.
        """
        super(LookupInResult, self).__init__(content.cas, content.rc)
        self._content = content  # type: CoreResult
        self.dict = kwargs

    @property
    def content_as(self):
        # type: (...) -> ContentProxySubdoc
        """
        Return a proxy that allows extracting the content as a provided type.

        Get first value as a string::

            value = cb.get('key').content_as[str](0)

        :return: returns as ContentProxySubdoc
        """
        return ContentProxySubdoc(self._content)

    def exists(self,
               index  # type: int
               ):
        return len(canonical_sdresult(self._content))>index


class MutationResult(Result):
    def __init__(self,
                core_result    # type: CoreResult
                ):
      super(MutationResult, self).__init__(core_result.cas, core_result.rc)
      mutinfo = getattr(core_result, '_mutinfo', None)
      muttoken = MutationToken(mutinfo) if mutinfo else None
      self.mutationToken = muttoken

    def mutation_token(self):
        # type: () -> MutationToken
        return self.mutationToken


class MutateInResult(MutationResult):
    @internal
    def __init__(self,
                 content,  # type: CoreResult
                 **options  # type: Any
                 ):
        # type: (...) -> None
        """
        MutateInResult is the return type for mutate_in operations.
        """
        super(MutateInResult,self).__init__(content)
        self._content = content  # type: CoreResult
        self.dict = options

    @property
    def content_as(self):
        # type: (...) -> ContentProxySubdoc
        """
        Return a proxy that allows extracting the content as a provided type.

        Get first result as a string::

            cb.mutate_in('user',
                          SD.array_addunique('tags', 'dog'),
                          SD.counter('updates', 1)).content_as[str](0)

        :return: returns a :class:`~.ContentProxySubdoc`
        """
        return ContentProxySubdoc(self._content)

    @property
    def key(self):
        # type: (...) -> str
        """ Original key of the operation """
        return self._content.key

class ExistsResult(Result):
  @internal
  def __init__(self,
               original # type: CoreResult
              ):
      super(ExistsResult, self).__init__(original.cas, original.rc)
      self._exists = (original.cas != 0)

  @property
  def exists(self):
      return self._exists

class GetResult(Result):
    @internal
    def __init__(self,
                 original,     # type: CoreResult,
                 expiry = None # type: timedelta
                ):
      """
      GetResult is the return type for full read operations.
      """
      super(GetResult, self).__init__(original.cas, original.rc)
      self._id = original.key
      self._original = original
      self._expiry = expiry

    def content_as_array(self):
        # type: (...) -> List
        return list(self.content)

    @property
    def id(self):
        # type: () -> str
        return self._id

    @property
    def expiry(self):
        # type: () -> timedelta
        return self._expiry

    @property
    def content_as(self):
        # type: (...) -> ContentProxy
        return ContentProxy(self._original)

    @property
    def content(self):
        # type: () -> Any
        return extract_value(self._original, lambda x: x)


T = TypeVar('T', bound=Tuple[ResultProtocol, ...])

class GetReplicaResult(GetResult):
  @property
  def is_replica(self):
    raise NotImplementedError("To be implemented in final sdk3 release")


class AsyncWrapper(object):
    @staticmethod
    def gen_wrapper(base):
        class Wrapped(base):
            def __init__(self,
                         core_result,
                         **kwargs):
                self._original = core_result
                self._kwargs = kwargs

            def set_callbacks(self, on_ok_orig, on_err_orig):
                def on_ok(res):
                    on_ok_orig(base(res, **self._kwargs))

                def on_err(res, excls, excval, exctb):
                    on_err_orig(res, excls, excval, exctb)

                self._original.set_callbacks(on_ok, on_err)

            def clear_callbacks(self, *args):
                self._original.clear_callbacks(*args)

        return Wrapped


class AsyncGetResult(AsyncWrapper.gen_wrapper(GetResult)):
    def __init__(self,
                 core_result  # type: CoreResult
                 ):
        super(AsyncGetResult, self).__init__(core_result)

class AsyncGetReplicaResult(AsyncWrapper.gen_wrapper(GetReplicaResult)):
    def __init__(self,
                 sdk2_result  # type: SDK2Result
                 ):
        super(AsyncGetReplicaResult, self).__init__(sdk2_result)

class AsyncMutationResult(AsyncWrapper.gen_wrapper(MutationResult)):
    def __init__(self,
                 core_result  # type: CoreResult
                 ):
        # type (...)->None
        super(AsyncMutationResult, self).__init__(core_result)


# TODO: eliminate the options shortly.  They serve no purpose
ResultPrecursor = NamedTuple('ResultPrecursor', [('orig_result', CoreResult), ('orig_options', Mapping[str, Any])])


def get_result_wrapper(func  # type: Callable[[Any], ResultPrecursor]
                       ):
    # type: (...) -> Callable[[Any], GetResult]
    @wraps(func)
    def wrapped(*args, **kwargs):
        x, options = func(*args, **kwargs)
        factory_class=AsyncGetResult if issubclass(type(x), AsyncResult) else GetResult
        return factory_class(x)

    wrapped.__name__ = func.__name__
    wrapped.__doc__ = func.__name__
    return wrapped

def get_replica_result_wrapper(func  # type: Callable[[Any], ResultPrecursor]
                       ):

    def factory_class(x):
        factory=AsyncGetReplicaResult if issubclass(type(x), AsyncResult) else GetReplicaResult
        return factory(x)

    # type: (...) -> Callable[[Any], GetResult]
    @wraps(func)
    def wrapped(*args, **kwargs):
        x = list(map(factory_class, func(*args, **kwargs)))
        if (len(x) > 1):
            return x
        return x[0]

    wrapped.__name__ = func.__name__
    wrapped.__doc__ = func.__name__
    return wrapped


class MutationToken(object):
    def __init__(self, token):
        token = token or (None, None, None)
        (self.vbucketId, self.vbucketUUID, self.sequenceNumber)=token

    def partition_id(self):
        # type: (...) -> int
        return self.vbucketId

    def partition_uuid(self):
        # type: (...) -> int
        return self.vbucketUUID

    def sequence_number(self):
        # type: (...) -> int
        return self.sequenceNumber

    def bucket_name(self):
        # type: (...) -> str
        raise NotImplementedError()


def get_mutation_result(result  # type: ResultPrecursor
                        ):
    # type (...)->MutationResult
    orig_result = getattr(result,'orig_result',result)
    factory_class = AsyncMutationResult if issubclass(type(orig_result), AsyncResult) else MutationResult
    return factory_class(orig_result)


def get_multi_mutation_result(target, wrapped, keys, *options, **kwargs):
    final_options = forward_args(kwargs, *options)
    raw_result = wrapped(target, keys, **final_options)
    return {k: get_mutation_result(ResultPrecursor(v, final_options)) for k, v in raw_result.items()}


def _wrap_in_mutation_result(func  # type: Callable[[Any,...],CoreResult]
                             ):
    # type: (...) -> Callable[[Any,...],MutationResult]
    @wraps(func)
    def mutated(*args, **kwargs):
        result = func(*args, **kwargs)
        return get_mutation_result(result)

    mutated.__name__ = func.__name__
    mutated.__doc__ = func.__doc__
    return mutated


class ViewResultProtocol(ResultProtocol, Protocol):
    @property
    @abstractmethod
    def error(self):
        pass

    @property
    @abstractmethod
    def success(self):
        pass

    @property
    @abstractmethod
    def cas(self):
        pass


class ViewResult(IterableWrapper):
    def __init__(self, core_view  # type: CoreView
                ):
        super(ViewResult, self).__init__(core_view)

    @property
    def error(self):
        return self.parent.errors

    @property
    def success(self):
        return not self.parent.errors

    @property
    def cas(self):
        raise NotImplementedError()



