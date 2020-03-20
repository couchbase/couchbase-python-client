#
# Copyright 2013, Couchbase, Inc.
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
from datetime import datetime, timedelta
from warnings import warn

import enum
import warnings
from collections import defaultdict
try:
    from abc import abstractmethod, ABCMeta
except:
    import abstractmethod

from copy import deepcopy
import logging
import traceback


# Pythons > (2.7||3.2) silence deprecation warnings by default.
# Many folks are not happy about this, as it avoids letting them
# know about potential upcoming breaking changes in their code.
# Here we add a warning filter for any deprecation warning thrown
# by Couchbase
from enum import Enum

warnings.filterwarnings(action='default',
                        category=DeprecationWarning,
                        module=r"^couchbase($|\..*)")

import couchbase_core._libcouchbase as _LCB

from typing import *

JSON = Union[str, int, float, bool, None, Mapping[str, 'JSON'], List['JSON']]

try:
    from couchbase_core._version import __version__

except ImportError:
    __version__ = "0.0.0-could-not-find-git"

try:
    from importlib import reload  # Python 3.4+
except ImportError:
    # Needed for Python 3.0-3.3; harmless in Python 2.7 where imp.reload is just an
    # alias for the builtin reload.
    from imp import reload

from types import ModuleType
import os, sys

try:
    StopAsyncIteration = StopAsyncIteration
except:
    StopAsyncIteration = StopIteration

from couchbase_core._pyport import ulp, with_metaclass


def set_json_converters(encode, decode):
    """
    Modify the default JSON conversion functions. This affects all
    :class:`~couchbase_core.client.Client` instances.

    These functions will called instead of the default ones (``json.dumps``
    and ``json.loads``) to encode and decode JSON (when :const:`FMT_JSON` is
    used).

    :param callable encode: Callable to invoke when encoding an object to JSON.
        This should have the same prototype as ``json.dumps``, with the
        exception that it is only ever passed a single argument.

    :param callable decode: Callable to invoke when decoding an object to JSON.
        This should have the same prototype and behavior
        as ``json.loads`` with the exception that it is only ever
        passed a single argument.

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised, and it is the responsibility of the caller to
    ensure that the provided functions operate correctly, otherwise exceptions
    may be thrown randomly when encoding and decoding values
    """
    ret = _LCB._modify_helpers(json_encode=encode, json_decode=decode)
    return (ret['json_encode'], ret['json_decode'])


def set_pickle_converters(encode, decode):
    """
    Modify the default Pickle conversion functions. This affects all
    :class:`~couchbase_core.client.Client` instances.

    These functions will be called instead of the default ones
    (``pickle.dumps`` and ``pickle.loads``) to encode and decode values to and
    from the Pickle format (when :const:`FMT_PICKLE` is used).

    :param callable encode: Callable to invoke when encoding an object to
        Pickle. This should have the same prototype as ``pickle.dumps`` with
        the exception that it is only ever called with a single argument

    :param callable decode: Callable to invoke when decoding a Pickle encoded
        object to a Python object. Should have the same prototype as
        ``pickle.loads`` with the exception that it is only ever passed a
        single argument

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised and it is the responsibility of the caller to
    ensure that the provided functions operate correctly.
    """
    ret = _LCB._modify_helpers(pickle_encode=encode, pickle_decode=decode)
    return (ret['pickle_encode'], ret['pickle_decode'])


def _to_json(*args):
    """
    Utility function to encode an object to json using the user-defined
    JSON encoder (see :meth:`set_json_converters`).

    :param args: Arguments passed to the encoder
    :return: Serialized JSON string
    """
    return _LCB._get_helper('json_encode')(*args)


def _from_json(*args):
    """
    Utility function to decode a JSON string to a Python object using
    the user-defined JSON decoder (see :meth:`set_json_converters`).

    :param args: Arguments passed to the decoder
    :return: Python object converted from JSON
    """
    return _LCB._get_helper('json_decode')(*args)


def enable_logging():
    """
    Enables integration with Python's `logging` module.

    This function enables the C library's logging to be propagated to
    the Python standard `logging` module.

    Calling this function affects any :class:`~couchbase_core.client.Client` objects created
    afterwards (but not before). Note that currently this will also
    override any ``LCB_LOGLEVEL`` directive inside the environment as
    well.

    The "root" couchbase_core logger is ``couchbase``.
    """
    import couchbase_core._logutil
    couchbase_core._logutil.configure(True)


def disable_logging():
    import couchbase_core._logutil
    couchbase_core._logutil.configure(False)


_enum_counts = defaultdict(lambda: 0)


def real_or_placeholder(cls, name):
    result = getattr(_LCB, name, _enum_counts[cls])
    _enum_counts[cls] += 1
    return result


class CompatibilityEnum(enum.Enum):
    @classmethod
    def prefix(cls):
        return ""

    def __init__(self, value=None):
        self.orig_value = value
        self._value_ = real_or_placeholder(type(self), type(self).prefix() + self._name_)

    def __int__(self):
        return self.value


T = TypeVar('T', bound=str)


class JSONMapping(object):
    def __init__(self,  # type: JSONMapping
                 raw_json  # type: Mapping[str, JSON]
                 ):

        self._raw_json = deepcopy(self.defaults())
        for k, v in raw_json.items():
            try:
                setattr(self, k, v)
            except:
                self._raw_json[k] = v

    @staticmethod
    def _genprop(dict_key  # type: str
                 ):
        # type: (...) -> property
        def fget(self):
            return self._raw_json.get(dict_key, None)

        def fset(self, val):
            self._raw_json[dict_key] = val

        def fdel(self):
            try:
                del self._raw_json[dict_key]
            except KeyError:
                pass

        return property(fget, fset, fdel)

    def defaults(self):
        return {}

class Mapped(with_metaclass(ABCMeta)):
    @classmethod
    def of(cls, *args, **kwargs):
        return Mapped._of(cls, *args, **kwargs)

    @staticmethod
    def _of(cls, *args, **kwargs):
        final_args = cls.defaults()
        final_args.update(*args)
        final_args.update({cls.mappings().get(k, k): v for k, v in kwargs.items()})
        try:
            return cls.factory(**final_args)
        except Exception as e:
            pass
    factory = None

    @staticmethod
    def mappings():
        return None

    @staticmethod
    def defaults():
        return None

def recursive_reload(module, paths=None, mdict=None):
    """Recursively reload modules."""
    if paths is None:
        paths = ['']
    if mdict is None:
        mdict = {}
    if module not in mdict:
        # modules reloaded from this module
        mdict[module] = []
    reload(module)
    for attribute_name in dir(module):
        attribute = getattr(module, attribute_name)
        if type(attribute) is ModuleType:
            if attribute not in mdict[module]:
                if attribute.__name__ not in sys.builtin_module_names:
                    if os.path.dirname(attribute.__file__) in paths:
                        mdict[module].append(attribute)
                        recursive_reload(attribute, paths, mdict)
    reload(module)


from couchbase_core.fulltext import SearchRequest
from couchbase_core.n1ql import N1QLRequest
from couchbase_core.views.iterator import View

IterableQuery = Union[SearchRequest, N1QLRequest, View]

WrappedIterable = TypeVar('T', bound=IterableQuery)


def iterable_wrapper(basecls  # type: Type[WrappedIterable]
                     ):
    # type: (...) -> Union[Type['IterableWrapper'], Type[WrappedIterable]]
    class IterableWrapper(basecls):
        def __init__(self,
                     *args, **kwargs  # type: IterableQuery
                     ):
            super(IterableWrapper, self).__init__(*args, **kwargs)
            self.done = False
            self.buffered_rows = []

        def rows(self):
            return list(x for x in self)

        def metadata(self):
            # type: (...) -> JSON
            return self.meta

        def __iter__(self):
            for row in self.buffered_rows:
                yield row
            parent_iter = super(IterableWrapper, self).__iter__()
            while not self.done:
                try:
                    next_item = next(parent_iter)
                    self.buffered_rows.append(next_item)
                    yield next_item
                except (StopAsyncIteration, StopIteration) as e:
                    self.done = True
                    break
    return IterableWrapper


def _depr(fn, usage, stacklevel=3):
    """Internal convenience function for deprecation warnings"""
    warn('{0} is deprecated. Use {1} instead'.format(fn, usage),
         stacklevel=stacklevel, category=DeprecationWarning)


def mk_formstr(d):
    l = []
    for k, v in d.items():
        l.append('{0}={1}'.format(ulp.quote(k), ulp.quote(str(v))))

    return '&'.join(l)


def syncwait_or_deadline_time(syncwait, timeout):
    deadline = (datetime.now() + timedelta(microseconds=timeout)) if timeout else None
    return lambda: syncwait if syncwait else (deadline - datetime.now()).total_seconds()
