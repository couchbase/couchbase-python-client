#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
                                  ServiceUnavailableException,
                                  UnAmbiguousTimeoutException)
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.exceptions import exception as CouchbaseBaseException


def decode_value(transcoder, value, flags, is_subdoc=False):
    if is_subdoc is False:
        return transcoder.decode_value(value, flags)

    final_value = []
    for f in value:
        if 'value' in f:
            tmp = copy(f)
            old = tmp.pop('value', None)
            if old:
                # no custom transcoder for subdoc ops, use JSON
                tmp['value'] = transcoder.decode_value(old, FMT_JSON)
            final_value.append(tmp)
        else:
            final_value.append(f)

    return final_value


def decode_replicas(transcoder, result, return_cls, is_subdoc=False):
    while True:
        try:
            res = next(result)
        except StopIteration:
            # this is a timeout from pulling a result from the queue, kill the generator
            raise UnAmbiguousTimeoutException('Timeout reached waiting for result in queue.') from None
        else:
            if isinstance(res, CouchbaseBaseException):
                raise ErrorMapper.build_exception(res)
            # should only be None once all replicas have been retrieved
            if res is None:
                return

            value = res.raw_result.get('value', None)
            flags = res.raw_result.get('flags', None)
            res.raw_result['value'] = decode_value(transcoder, value, flags, is_subdoc=is_subdoc)
            yield return_cls(res)


class BlockingWrapper:
    @classmethod  # noqa: C901
    def block(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    ret = fn(self, *args, **kwargs)
                    if isinstance(ret, BaseCouchbaseException):
                        raise ErrorMapper.build_exception(ret)
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        if ret is None:
                            raise InternalSDKException('Expected return value to be non-empty.')
                        retval = return_cls(ret)
                    return retval
                except CouchbaseException as e:
                    if isinstance(e, ServiceUnavailableException) and fn.__name__ == '_get_cluster_info':
                        e._message = ('If using Couchbase Server < 6.6, '
                                      'a bucket needs to be opened prior to cluster level operations')
                    raise e
                except Exception as ex:
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
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
                    if isinstance(ret, BaseCouchbaseException):
                        raise ErrorMapper.build_exception(ret)

                    is_subdoc = fn.__name__ in [
                        '_lookup_in_internal', '_lookup_in_any_replica_internal', '_lookup_in_all_replicas_internal'
                    ]

                    # special case for get_all_replicas and lookup_in_all_replicas
                    if fn.__name__ in ['_get_all_replicas_internal', '_lookup_in_all_replicas_internal']:
                        return decode_replicas(transcoder, ret, return_cls, is_subdoc=is_subdoc)

                    value = ret.raw_result.get('value', None)
                    flags = ret.raw_result.get('flags', None)

                    ret.raw_result['value'] = decode_value(transcoder, value, flags, is_subdoc=is_subdoc)
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        retval = return_cls(ret)
                    return retval
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
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
