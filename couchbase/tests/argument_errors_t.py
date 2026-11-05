#  Copyright 2016-2026. Couchbase, Inc.
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

import importlib
import os
import pkgutil
import re
from collections import namedtuple
from collections.abc import Callable
from enum import Enum
from typing import Any

import pytest

import couchbase
import couchbase.common._transforms as transforms
from couchbase.exceptions import InvalidArgumentException

# path:    the enum, module first, so the row also says where the converter lives.
# method:  from_str for the caller-facing family, from_server / from_server_str / from_int for the
#          family that parses a response.  The name is the only place the SDK records which one a
#          converter belongs to.
# raises:  the exception type an unrecognised value produces, or None where the converter answers
#          with default instead.
# message: the exact text, as a template over value and name, or None where only the type is pinned.
# default: the member an unrecognised value becomes, for the converters that do not raise.
# fed_by:  where the values actually reaching this converter come from, which is the axis any
#          convergence has to run on and is asserted by test_call_site_inventory_is_unchanged.
_Converter = namedtuple('_Converter', ['path', 'method', 'raises', 'message', 'default', 'fed_by'])

_UNKNOWN_VALUES = ['not-a-real-value', 17, None]

# The message the six strict from_str converters share.  Grepping for its tail is how the
# option-validation sweep counts the sites it has to reach, so the text is a contract and not a
# diagnostic.
_STRICT_MESSAGE = '{value} is not a valid {name} option. Expected str representation of type {name}.'

# Four answers to one question, which is the point of listing them in a single table: a value the
# converter cannot name raises the SDK's exception under the shared message in six places, raises
# it under a text of its own in one more, becomes an explicit unknown member in thirteen, and
# becomes a member carrying a substantive meaning the caller never asked for in three.
#
# The split that matters is fed_by, not method: three converters in the caller-facing family are
# fed values the SDK was handed, and DesignDocumentNamespace is fed both.
_CONVERTERS = [
    _Converter('couchbase.common.analytics.AnalyticsStatus',
               'from_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.n1ql.QueryStatus',
               'from_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.option_enums.IpProtocol',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.config_profiles.KnownConfigProfiles',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.option_enums.TLSVerifyMode',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.views.ViewScanConsistency',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.auth.AuthDomain',
               'from_str', None, None, 'Local', 'server'),
    _Converter('couchbase.common.views.ViewErrorMode',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.views.ViewOrdering',
               'from_str', 'InvalidArgumentException', _STRICT_MESSAGE, None, 'caller'),
    _Converter('couchbase.common.management.views.DesignDocumentNamespace',
               'from_str', None, None, 'DEVELOPMENT', 'caller and server'),
    _Converter('couchbase.common.durability.DurabilityLevel',
               'from_server_str', None, None, 'NONE', 'server'),
    _Converter('couchbase.common.management.buckets.CompressionMode',
               'from_server_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.management.buckets.StorageBackend',
               'from_server_str', None, None, 'UNDEFINED', 'server'),
    _Converter('couchbase.common.management.buckets.BucketType',
               'from_server_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.management.buckets.ConflictResolutionType',
               'from_server_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.management.buckets.EvictionPolicyType',
               'from_server_str', None, None, 'UNKNOWN', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionBucketAccess',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionDcpBoundary',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionDeploymentStatus',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionLanguageCompatibility',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionLogLevel',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionProcessingStatus',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.management.eventing.EventingFunctionState',
               'from_server', None, None, 'Unknown', 'server'),
    _Converter('couchbase.common.option_enums.Compression',
               'from_int', 'InvalidArgumentException',
               'cannot convert {value} to a Compression', None, 'nothing in the tree'),
]

_CONVERTER_IDS = [f'{c.path.rsplit(".", 1)[1]}.{c.method}' for c in _CONVERTERS]

_STRICT_CONVERTERS = [c for c in _CONVERTERS if c.message is _STRICT_MESSAGE]
_STRICT_IDS = [c.path.rsplit('.', 1)[1] for c in _STRICT_CONVERTERS]

_MESSAGE_CONVERTERS = [c for c in _CONVERTERS if c.message is not None]
_MESSAGE_IDS = [f'{c.path.rsplit(".", 1)[1]}.{c.method}' for c in _MESSAGE_CONVERTERS]

# Every place in the package that hands a value to one of the converters above, as
# (module, ClassName.method).  A converter fed from two provenances cannot answer both of them the
# same way, so a new row here is a question about that row and not a formality;
# DesignDocumentNamespace is the one that already has two.
_CALL_SITES = {
    ('couchbase/protocol/classic/_metadata.py', 'AnalyticsStatus.from_str'),
    ('couchbase/protocol/classic/_metadata.py', 'QueryStatus.from_str'),
    ('couchbase/protocol/classic/_views.py', 'ViewScanConsistency.from_str'),
    ('couchbase/protocol/classic/_views.py', 'ViewOrdering.from_str'),
    ('couchbase/protocol/classic/_views.py', 'ViewErrorMode.from_str'),
    ('couchbase/protocol/classic/_views.py', 'DesignDocumentNamespace.from_str'),
    ('couchbase/common/management/users.py', 'AuthDomain.from_str'),
    ('couchbase/common/management/views.py', 'DesignDocumentNamespace.from_str'),
    ('couchbase/common/management/buckets.py', 'BucketType.from_server_str'),
    ('couchbase/common/management/buckets.py', 'CompressionMode.from_server_str'),
    ('couchbase/common/management/buckets.py', 'StorageBackend.from_server_str'),
    ('couchbase/common/management/buckets.py', 'ConflictResolutionType.from_server_str'),
    ('couchbase/common/management/buckets.py', 'EvictionPolicyType.from_server_str'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionBucketAccess.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionDcpBoundary.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionDeploymentStatus.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionLanguageCompatibility.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionLogLevel.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionProcessingStatus.from_server'),
    ('couchbase/common/management/eventing.py', 'EventingFunctionState.from_server'),
}

# The argument validators that carry a message of their own.
_VALIDATORS = [
    (transforms.validate_int, 'x', 'Expected value to be of type int.'),
    (transforms.validate_bool, 'x', 'Expected value to be of type bool.'),
    (transforms.validate_str, 1, 'Expected value to be of type str.'),
    (transforms.to_milliseconds, 'x', 'Expected timeout to be of type timedelta | float | int instead of x'),
    (transforms.timedelta_as_milliseconds, 'x', "Expected timedelta instead of <class 'str'>"),
]

_VALIDATOR_IDS = [f'{fn.__module__.rsplit(".", 1)[1]}.{fn.__name__}' for fn, _, _ in _VALIDATORS]

# The packages behind an extra, which the modules that use them import at the top, so an install
# without the extra cannot import those modules.
_OPTIONAL_DEPENDENCIES = frozenset({'opentelemetry'})

_CONVERTER_METHODS = ('from_str', 'from_server_str', 'from_server', 'from_int')


def _resolve(path: str) -> Any:
    module_name, class_name = path.rsplit('.', 1)
    return getattr(importlib.import_module(module_name), class_name)


class ArgumentErrorUnitTests:
    """Characterization of what the SDK does with a value it cannot interpret, and of the text.

    Two groups, and the difference between them is the whole subject.  The 23 enum converters
    answer an unrecognised value in four incompatible ways, three of which are to answer with a
    member the caller never asked for, and the family name that would tell them apart
    (from_str against from_server) does not line up with where the values come from.  The argument
    validators answer in one way, with a message that the option-validation sweep indexes by.
    Everything here is asserted against the value it produces today rather than the value it ought
    to produce; changing either group is a decision, and this file is what makes it one.

    Nothing here touches a cluster.
    """

    # No TEST_MANIFEST: the converters and validators have one definition, shared by all three apis,
    # so there is nothing for a generated copy to be in parity with.

    @pytest.mark.parametrize('converter', _CONVERTERS, ids=_CONVERTER_IDS)
    def test_converter_answers_an_unknown_value(self, converter: _Converter) -> None:
        enum_cls = _resolve(converter.path)
        convert = getattr(enum_cls, converter.method)
        if converter.raises is None:
            assert convert('not-a-real-value') is getattr(enum_cls, converter.default)
        else:
            with pytest.raises(Exception) as exc_info:
                convert('not-a-real-value')
            assert type(exc_info.value).__name__ == converter.raises

    @pytest.mark.parametrize('converter', _CONVERTERS, ids=_CONVERTER_IDS)
    def test_converter_treats_a_non_string_as_an_unknown_string(self, converter: _Converter) -> None:
        enum_cls = _resolve(converter.path)
        convert = getattr(enum_cls, converter.method)
        for value in _UNKNOWN_VALUES[1:]:
            if converter.raises is None:
                assert convert(value) is getattr(enum_cls, converter.default)
            else:
                with pytest.raises(Exception) as exc_info:
                    convert(value)
                assert type(exc_info.value).__name__ == converter.raises

    @pytest.mark.parametrize('converter', _MESSAGE_CONVERTERS, ids=_MESSAGE_IDS)
    def test_converter_message(self, converter: _Converter) -> None:
        enum_cls = _resolve(converter.path)
        convert = getattr(enum_cls, converter.method)
        for value in _UNKNOWN_VALUES:
            with pytest.raises(InvalidArgumentException) as exc_info:
                convert(value)
            assert exc_info.value.message == converter.message.format(value=value, name=enum_cls.__name__)

    @pytest.mark.parametrize('converter', _STRICT_CONVERTERS, ids=_STRICT_IDS)
    def test_strict_converters_share_one_message(self, converter: _Converter) -> None:
        enum_cls = _resolve(converter.path)
        assert converter.message is _STRICT_MESSAGE
        with pytest.raises(InvalidArgumentException) as exc_info:
            getattr(enum_cls, converter.method)('not-a-real-value')
        assert exc_info.value.message == _STRICT_MESSAGE.format(value='not-a-real-value',
                                                                name=enum_cls.__name__)

    # The only converter that normalises before it looks.  Every call site reads the value out of a
    # server response with .get and no default, so None is reachable and must not reach .upper().
    def test_log_level_upper_cases_before_it_checks(self) -> None:
        from couchbase.common.management.eventing import EventingFunctionLogLevel
        assert EventingFunctionLogLevel.from_server('info') is EventingFunctionLogLevel.Info
        not_a_str: Any = None
        assert EventingFunctionLogLevel.from_server(not_a_str) is EventingFunctionLogLevel.Unknown

    # A converter added without a row here would be unclassified rather than a failure, and an
    # unclassified converter is how a ninth answer to the question arrives unnoticed.
    def test_converter_inventory_is_complete(self) -> None:
        found: set[tuple[str, str]] = set()
        for module_info in pkgutil.walk_packages(couchbase.__path__, 'couchbase.'):
            if module_info.name.startswith('couchbase.tests'):
                continue
            try:
                module = importlib.import_module(module_info.name)
            except ModuleNotFoundError as e:
                if (e.name or '').partition('.')[0] in _OPTIONAL_DEPENDENCIES:
                    continue
                raise
            for obj in vars(module).values():
                if not isinstance(obj, type):
                    continue
                for method in _CONVERTER_METHODS:
                    if method in vars(obj) and issubclass(obj, Enum):
                        found.add((f'{obj.__module__}.{obj.__qualname__}', method))
        assert found == set((c.path, c.method) for c in _CONVERTERS)

    # Provenance is not readable off the converter, so it is read off the callers.  A new row is a
    # value arriving from somewhere the converter's answer was not chosen for.
    def test_call_site_inventory_is_unchanged(self) -> None:
        names = {c.path.rsplit('.', 1)[1]: c.method for c in _CONVERTERS}
        # No trailing paren, so a converter handed on as a callable is a call site too.
        pattern = re.compile(r'\b(' + '|'.join(names) + r')\.(' + '|'.join(_CONVERTER_METHODS) + r')\b')
        root = os.path.dirname(os.path.dirname(os.path.abspath(couchbase.__file__)))
        found: set[tuple[str, str]] = set()
        for dirpath, dirnames, filenames in os.walk(os.path.dirname(couchbase.__file__)):
            dirnames[:] = [d for d in dirnames if not d.startswith('tests')]
            for filename in filenames:
                if not filename.endswith('.py'):
                    continue
                path = os.path.join(dirpath, filename)
                with open(path, encoding='utf-8') as source_file:
                    source = source_file.read()
                for class_name, method in pattern.findall(source):
                    # _CALL_SITES is written with '/', which relpath does not give on Windows.
                    rel = os.path.relpath(path, root).replace(os.sep, '/')
                    found.add((rel, f'{class_name}.{method}'))
        assert found == _CALL_SITES

    @pytest.mark.parametrize('validator, value, message', _VALIDATORS, ids=_VALIDATOR_IDS)
    def test_validator_message(self, validator: Callable[[Any], Any], value: Any, message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            validator(value)
        assert exc_info.value.message == message
