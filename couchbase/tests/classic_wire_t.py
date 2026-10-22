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
from datetime import timedelta
from decimal import Decimal

import pytest

from couchbase.common import _transforms
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common._transforms import MAX_EXPIRY_TIMESTAMP
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.options import GetAndLockMultiOptions
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.warnings import CouchbaseApplicabilityWarning
from couchbase.protocol.classic._wire import (forward_args,
                                              get_valid_multi_args,
                                              to_wire)
from tests.test_features import TestProtocol

# What is checked here is classic's map from an operation's options to what its binding reads.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

# Classic declares it inert on ClusterOptions, and couchbase2 declares nothing.
INERT_KEY = 'compression'

CAS_BLOCKS = ['AppendOptions', 'MutateInOptions', 'PrependOptions', 'RemoveOptions', 'ReplaceOptions', 'UnlockOptions',
              'AppendMultiOptions', 'PrependMultiOptions', 'RemoveMultiOptions', 'ReplaceMultiOptions']
NOW = 1_800_000_000


class _Couchbase2Blocks(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    Cluster = 'ClusterOptions'


class _ClassicBlocks(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    Cluster = 'ClusterOptions'


class ClassicWireUnitTests:

    @pytest.mark.parametrize('block, reported', [
        (_ClassicBlocks.Cluster, 1),
        (_Couchbase2Blocks.Cluster, 0),
    ])
    def test_forwarding_reads_the_protocol_off_the_block(self, block: OptionsBlock, reported: int) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            forward_args({INERT_KEY: True}, block=block, mode=OptionValidationMode.Warn)
        assert len([w for w in caught if issubclass(w.category, CouchbaseApplicabilityWarning)]) == reported

    # Regression guard (PYCBC-1957): the multi filter used to drop the lock time.
    def test_get_and_lock_multi_sends_its_lock_time(self) -> None:
        final_args = get_valid_multi_args(GetAndLockMultiOptions, {'lock_time': timedelta(seconds=5)}, None,
                                          protocol=CouchbaseProtocol.CLASSIC)
        assert final_args['lock_time'] == 5

    @pytest.mark.parametrize('block', CAS_BLOCKS)
    @pytest.mark.parametrize('cas', ['1234', 1234.0, Decimal(1234)])
    def test_a_cas_that_is_not_an_int_raises(self, block: str, cas: object) -> None:
        with pytest.raises(InvalidArgumentException):
            to_wire({'cas': cas}, block)

    @pytest.mark.parametrize('block', CAS_BLOCKS)
    def test_an_int_cas_is_sent(self, block: str) -> None:
        assert to_wire({'cas': 1234}, block) == {'cas': 1234}

    def test_the_last_expiry_the_server_can_hold_is_sent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_transforms, 'time', lambda: NOW)
        expiry = timedelta(seconds=MAX_EXPIRY_TIMESTAMP - NOW)
        assert to_wire({'expiry': expiry}, 'UpsertOptions') == {'expiry': MAX_EXPIRY_TIMESTAMP}

    @pytest.mark.parametrize('expiry', [timedelta(seconds=MAX_EXPIRY_TIMESTAMP - NOW + 1), timedelta(days=36500)])
    def test_an_expiry_past_what_the_server_can_hold_raises(self,
                                                            monkeypatch: pytest.MonkeyPatch,
                                                            expiry: timedelta) -> None:
        monkeypatch.setattr(_transforms, 'time', lambda: NOW)
        with pytest.raises(InvalidArgumentException):
            to_wire({'expiry': expiry}, 'UpsertOptions')
