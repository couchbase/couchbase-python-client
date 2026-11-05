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

import warnings

import pytest

from couchbase.common._option_diagnostics import OptionsBlock, check_options
from couchbase.common._option_spec import BLOCKS
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.options import (DeltaValue,
                                      SignedInt64,
                                      UnsignedInt32,
                                      UnsignedInt64)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.warnings import CouchbaseApplicabilityWarning, CouchbaseDeprecationWarning

# Classic declares it inert on ClusterOptions, and couchbase2 declares nothing.
INERT_KEY = 'compression'


def _applicability(protocol: CouchbaseProtocol | None) -> list[warnings.WarningMessage]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        check_options('ClusterOptions', [INERT_KEY], OptionValidationMode.Warn, protocol=protocol)
    return [w for w in caught if issubclass(w.category, CouchbaseApplicabilityWarning)]


class OptionDiagnosticsUnitTests:

    def test_the_key_is_inert_on_classic_alone(self) -> None:
        assert BLOCKS['ClusterOptions'].keys[INERT_KEY].inert == frozenset({'classic'})

    def test_an_inert_key_is_reported_on_the_protocol_that_declares_it(self) -> None:
        assert len(_applicability(CouchbaseProtocol.CLASSIC)) == 1

    def test_an_inert_key_is_not_reported_on_another_protocol(self) -> None:
        assert _applicability(CouchbaseProtocol.COUCHBASE2) == []

    def test_no_protocol_judges_everything_but_applicability(self) -> None:
        assert _applicability(None) == []

    def test_strict_raises_on_the_protocol_that_declares_it(self) -> None:
        with pytest.raises(InvalidArgumentException):
            check_options('ClusterOptions', [INERT_KEY], OptionValidationMode.Strict,
                          protocol=CouchbaseProtocol.CLASSIC)

    def test_a_block_enum_has_to_name_its_protocol(self) -> None:
        with pytest.raises(TypeError):
            class _Unnamed(OptionsBlock):
                Cluster = 'ClusterOptions'


class OptionValueUnitTests:

    @pytest.mark.parametrize('cls', [DeltaValue, SignedInt64, UnsignedInt64])
    def test_a_constrained_int_prints_its_class_name(self, cls: type) -> None:
        value = cls(5)
        assert str(value) == repr(value) == f'{cls.__name__}(5)'

    # Regression guard (PYCBC-1952): query and analytics metrics are UnsignedInt64 values.
    @pytest.mark.parametrize('cls', [DeltaValue, SignedInt64, UnsignedInt64])
    def test_a_constrained_int_behaves_as_its_int(self, cls: type) -> None:
        value = cls(5)
        assert value == 5
        assert value == cls(5)
        assert hash(value) == hash(cls(5))
        assert {value: 'found'}[cls(5)] == 'found'
        total = value + 2
        assert isinstance(total, cls)
        assert total == 7

    def test_unsigned_int32_is_deprecated(self) -> None:
        with pytest.warns(CouchbaseDeprecationWarning, match='UnsignedInt32'):
            value = UnsignedInt32(5)
        assert value == 5
