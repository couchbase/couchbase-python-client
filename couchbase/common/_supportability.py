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

import sys
import warnings
from collections.abc import Callable
from inspect import Signature
from typing import (Any,
                    NoReturn,
                    TypeVar)

from couchbase.common.warnings import CouchbaseDeprecationWarning

_SDK_PACKAGES = frozenset(('couchbase', 'acouchbase', 'txcouchbase'))
_TEST_PACKAGE = 'tests'

DURABILITY_LEVEL_READ = 'The SDK returns a DurabilityLevel member wherever it reads a durability level from the server.'

_T = TypeVar('_T')


def _caller_stacklevel() -> int:
    """The depth, counted from the helper that calls this, of the first frame outside the SDK.

    `warnings.warn` keeps its de-duplication registry in the frame the warning is attributed
    to, so a depth landing on an SDK line lets the first user call site consume the entry and
    silences every other site carrying the same message.  A constant cannot serve: the
    `TermRangeQuery.start_inclusive` warning is one frame deeper when the constructor sets the
    property than when the caller does.  The SDK's own test packages
    count as outside, so a test can assert on where a warning points.
    """
    try:
        frame = sys._getframe(2)
    except ValueError:
        return 2

    level = 2
    while frame is not None:
        parts = frame.f_globals.get('__name__', '').split('.', 2)
        if parts[0] not in _SDK_PACKAGES or (len(parts) > 2 and parts[1] == _TEST_PACKAGE):
            break
        if frame.f_back is None:
            break
        frame = frame.f_back
        level += 1
    return level


class Supportability:
    @classmethod
    def import_deprecated(cls, old_path: str, new_path: str) -> Callable[[type[_T]], type[_T]]:
        def decorator(cls: type[_T]) -> type[_T]:
            old_init = cls.__init__

            def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
                msg = (f"Importing {cls.__name__} from {old_path} is deprecated "
                       "and will be removed in a future release. "
                       f" Import {cls.__name__} from {new_path} instead.")
                warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())
                old_init(self, *args, **kwargs)

            # setattr because a type checker refuses replacing a method by assignment.
            setattr(cls, '__init__', new_init)
            return cls
        return decorator

    @classmethod
    def class_deprecated(cls, use_instead: str) -> Callable[[type[_T]], type[_T]]:
        def decorator(cls: type[_T]) -> type[_T]:
            old_init = cls.__init__

            def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
                msg = (f"Class {cls.__name__} is deprecated "
                       "and will be removed in a future release. "
                       f"Use {use_instead} instead.")
                warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())
                old_init(self, *args, **kwargs)

            # setattr because a type checker refuses replacing a method by assignment.
            setattr(cls, '__init__', new_init)
            return cls
        return decorator

    @staticmethod
    def class_property_deprecated(property: str,
                                  use_instead: str | None = None,
                                  message: str | None = None,
                                  ) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided class property is deprecated.

        Args:
            property (str): The name of the deprecated property
            use_instead (Optional, str): The name of the property to use instead of the deprecated property.
            message (Optional, str): A message to have in the warning to add context.
        """
        msg = f"Class property {property} is deprecated and will be removed in a future release."
        if use_instead:
            msg += f" Use {use_instead} instead."
        if message:
            msg += f" {message}"
        warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def method_deprecated(method: str, use_instead: str | None = None, message: str | None = None) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided method is deprecated.

        Args:
            method (str): The name of the deprecated method
            use_instead (Optional, str): The name of the method to use instead of the deprecated method.
            message (Optional, str): A message to have in the warning to add context.
        """
        msg = f"Method {method} is deprecated and will be removed in a future release."
        if use_instead:
            msg += f" Use {use_instead} instead."
        if message:
            msg += f" {message}"
        warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def method_param_deprecated(param: str, use_instead: str) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided param is deprecated.

        Args:
            param (str): The name of the deprecated param
            use_instead (str): The name of the param to use instead of the deprecated param.
        """
        message = (f"Method parameter {param} is deprecated and will be removed in a future release. "
                   f"Use {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def method_param_type_deprecated(method: str, param: str, type_: str, use_instead: str) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided type is deprecated for the
        provided parameter.

        Args:
            method (str): The name of the method.
            param (str): The name of the parameter.
            type_ (str): The name of the type that is deprecated for that parameter.
            use_instead (str): The name of the type to pass instead of the deprecated type.
        """
        message = (f"Passing a {type_} as the {param} parameter of {method} is deprecated and will be "
                   f"removed in a future release. Use {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def method_kwarg_deprecated(kwarg: str, use_instead: str) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided param is deprecated.

        Args:
            kwarg (str): The name of the deprecated key-word argument.
            use_instead (str): The name of the param to use instead of the deprecated param.
        """
        message = (f"Method key-word (kwarg) parameter {kwarg} is deprecated and will be removed in a future release. "
                   f"Use {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def method_signature_deprecated(method_name: str, signature: Signature, use_instead: Signature) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the method overload with the provided signature is
        deprecated.

        Args
            method_name (str): The name of the method
            signature (Signature): The signature of the deprecated overload
            use_instead (Signature): The signature of the overload that should be used instead.

        """
        message = (f"Method {method_name} with signature {signature} is deprecated and will be removed in "
                   f"a future release. Use {method_name} with signature {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def option_deprecated(param: str, use_instead: str | None = None, message: str | None = None) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided param is deprecated.

        Args:
            param (str): The name of the deprecated param
            use_instead (Optional, str): The name of the param to use instead of the deprecated param.
            message (Optional, str): A message to have in the warning to add context.
        """
        msg = f"Option {param} is deprecated and will be removed in a future release. "
        if use_instead:
            msg += f"Use {use_instead} instead. "
        if message:
            msg += message

        warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())

    @staticmethod
    def type_deprecated(type_: str, use_instead: str | None = None, message: str | None = None) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided type is deprecated.

        Args:
            param (str): The name of the deprecated type
            use_instead (Optional, str): The name of the type to use instead of the deprecated type.
            message (Optional, str): A message to have in the warning to add context.
        """
        msg = f"SDK type {type_} is deprecated and will be removed in a future release. "
        if use_instead:
            msg += f"Use {use_instead} instead. "
        if message:
            msg += message

        warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=_caller_stacklevel())


class RemoveProperty:
    """Used to override a get descriptor for a class property and raise an AttributeError to prevent access.

    This helper class should only be used in **rare** instances.  Specifically, it allows for an inheritance
    structure to remain intact while removing a property from a subclass.  In an ideal scenario, the hierarchy
    structure is revisted, but in some cases it is easier to keep the current structure.  This class provides a
    work-around.
    """

    def __init__(self, prop: str) -> None:
        self._prop = prop

    def __get__(self, instance: object, cls: type) -> NoReturn:
        raise AttributeError(f'Property "{self._prop}" is no longer a part of the {cls.__name__} class.')
