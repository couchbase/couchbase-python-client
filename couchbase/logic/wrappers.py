from __future__ import annotations

from copy import copy
from functools import wraps

from couchbase.constants import FMT_JSON
from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  ErrorMapper,
                                  ExceptionMap,
                                  InternalSDKException,
                                  PathNotFoundException,
                                  PycbcException)


def decode_value(transcoder, value, flags, is_subdoc=False):
    # if no flags, just assume default
    if not flags:
        flags = FMT_JSON

    if is_subdoc is False:
        return transcoder.decode_value(value, flags)

    final_value = []
    for f in value:
        if 'value' in f:
            tmp = copy(f)
            old = tmp.pop('value', None)
            if old:
                tmp['value'] = transcoder.decode_value(old, flags)
            final_value.append(tmp)
        else:
            final_value.append(f)

    return final_value


class BlockingWrapper:
    @classmethod  # noqa: C901
    def block(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    ret = fn(self, *args, **kwargs)
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        if ret is None:
                            raise InternalSDKException('Expected return value to be non-empty.')
                        retval = return_cls(ret)
                    return retval
                except PycbcException as e:
                    print(e.context)
                    print(e.error_code)
                    print(e.exc_info)
                    if e.context:
                        excptn = ErrorMapper.parse_error_context(e, excptn_msg=e.message)
                    else:
                        exc_cls = PYCBC_ERROR_MAP.get(e.error_code, CouchbaseException)
                        excptn = exc_cls(message=e.message)
                    # we are creating a new exception on purpose
                    raise excptn from None
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    print(f'base exception: {ex}')
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
                    print(exc_cls.__name__)
                    excptn = exc_cls(message=str(ex))
                    raise excptn from None

            return wrapped_fn
        return decorator

    @classmethod
    def block_and_decode(cls, return_cls):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    transcoder = kwargs.pop('transcoder')
                    ret = fn(self, *args, **kwargs)
                    value = ret.raw_result.get('value', None)
                    flags = ret.raw_result.get('flags', None)

                    is_suboc = fn.__name__ == '_lookup_in_internal'
                    ret.raw_result['value'] = decode_value(transcoder, value, flags, is_subdoc=is_suboc)
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        retval = return_cls(ret)
                    return retval
                except PycbcException as e:
                    print(e.context)
                    print(e.error_code)
                    print(e.exc_info)
                    if e.context:
                        excptn = ErrorMapper.parse_error_context(e, excptn_msg=e.message)
                    else:
                        exc_cls = PYCBC_ERROR_MAP.get(e.error_code, CouchbaseException)
                        excptn = exc_cls(message=e.message)
                    # we are creating a new exception on purpose
                    raise excptn from None
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    print(f'base exception: {ex}')
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
                    print(exc_cls.__name__)
                    excptn = exc_cls(message=str(ex))
                    raise excptn

            return wrapped_fn
        return decorator

    @classmethod
    def datastructure_op(cls, create_type=None):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    return fn(self, *args, **kwargs)
                except DocumentNotFoundException:
                    if create_type is not None:
                        try:
                            self._collection.insert(self._key, create_type())
                        except DocumentExistsException:
                            pass
                        return fn(self, *args, **kwargs)
                    else:
                        raise

            return wrapped_fn
        return decorator

    @classmethod   # noqa: C901
    def _dsop(cls, create_type=None, wrap_missing_path=True):   # noqa: C901
        """
            ** DEPRECATED **
        """
        def real_decorator(fn):
            @wraps(fn)
            def newfn(self, key, *args, **kwargs):
                try:
                    return fn(self, key, *args, **kwargs)
                except DocumentNotFoundException:
                    if kwargs.get('create'):
                        try:
                            if create_type == 'list':
                                self.insert(key, list())
                            elif create_type == 'dict':
                                self.insert(key, dict())
                        except DocumentExistsException:
                            pass
                        return fn(self, key, *args, **kwargs)
                    else:
                        raise
                except PathNotFoundException:
                    if wrap_missing_path:
                        raise IndexError(args[0])

            return newfn

        return real_decorator
