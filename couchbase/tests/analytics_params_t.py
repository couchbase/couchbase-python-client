#  Copyright 2016-2024. Couchbase, Inc.
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

from datetime import timedelta
from typing import Any

import pytest

from couchbase.analytics import (AnalyticsQuery,
                                 AnalyticsScanConsistency,
                                 AnalyticsStatus)
from couchbase.options import AnalyticsOptions
from couchbase.serializer import DefaultJsonSerializer


class AnalyticsParamsUnitTests:
    # No cb_env and no TEST_MANIFEST: every test builds an AnalyticsQuery and reads the dict
    # it would hand the core.  Not generated either: AnalyticsOptions and AnalyticsQuery are
    # the same objects under all three apis, imported from the same module.

    Q_STR = 'SELECT * FROM default'

    @staticmethod
    def _expected(**overrides: Any) -> dict[str, Any]:
        # `metrics` is in the base because create_query_object defaults it to True whether or
        # not the options asked for it.
        return {'statement': AnalyticsParamsUnitTests.Q_STR, 'metrics': True, **overrides}

    def test_encoded_consistency(self) -> None:
        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.REQUEST_PLUS)
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.REQUEST_PLUS.value
        assert query.consistency == AnalyticsScanConsistency.REQUEST_PLUS.value

        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.NOT_BOUNDED)
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.NOT_BOUNDED.value
        assert query.consistency == AnalyticsScanConsistency.NOT_BOUNDED.value

    def test_params_base(self) -> None:
        query = AnalyticsQuery.create_query_object(self.Q_STR, AnalyticsOptions())
        assert query.params == self._expected()

    def test_params_client_context_id(self) -> None:
        q_opts = AnalyticsOptions(client_context_id='test-string-id')
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params == self._expected(client_context_id='test-string-id')

    def test_params_priority(self) -> None:
        q_opts = AnalyticsOptions(priority=True)
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params == self._expected(priority=True)

    def test_params_query_context(self) -> None:
        q_opts = AnalyticsOptions(query_context='bucket.scope')
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        # The encoded key is `scope_qualifier`, not `query_context`.  The option is named for
        # the rfc and the wire field is named for what the core calls it, and the rename
        # happens in the query_context property setter rather than anywhere visible from here.
        assert query.params == self._expected(scope_qualifier='bucket.scope')

    def test_params_read_only(self) -> None:
        q_opts = AnalyticsOptions(read_only=True)
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        # `readonly`, one word, for the same reason `scope_qualifier` above is not
        # `query_context`.
        assert query.params == self._expected(readonly=True)

    def test_params_serializer(self) -> None:
        serializer = DefaultJsonSerializer()
        q_opts = AnalyticsOptions(serializer=serializer)
        query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params == self._expected(serializer=serializer)

    def test_params_timeout(self) -> None:
        # A timedelta, a whole number of seconds, and a fractional one, all reaching the same
        # conversion.  Held in a table annotated `Any` because the option signature declares
        # `Optional[timedelta]` while the runtime accepts a bare number too, and nothing is
        # going to reconcile the two, which would make a suppression here permanent.
        timeouts: list[tuple[Any, int]] = [(timedelta(seconds=120), 120000),
                                           (20, 20000),
                                           (25.5, 25500)]
        for timeout, expected_ms in timeouts:
            q_opts = AnalyticsOptions(timeout=timeout)
            query = AnalyticsQuery.create_query_object(self.Q_STR, q_opts)
            assert query.params == self._expected(timeout=expected_ms)

    def test_params_timeout_getter(self) -> None:
        # Regression guard (PYCBC-1894): the request carries milliseconds and the property
        # reports seconds, so the getter converts rather than handing back what is stored.  The
        # absent case has to stay None rather than becoming zero.
        query = AnalyticsQuery.create_query_object(self.Q_STR, AnalyticsOptions(timeout=timedelta(seconds=120)))
        assert query.timeout == 120

        query = AnalyticsQuery.create_query_object(self.Q_STR, AnalyticsOptions())
        assert query.timeout is None

    # This test cannot fail, and is left as it stands rather than corrected here: both the
    # parametrize argument and the lookup come from AnalyticsStatus.__members__, so
    # `AnalyticsStatus[value]` is `expected` by construction whatever the enum contains.
    # Making it mean something means asserting the expected member set and each wire value.
    @pytest.mark.parametrize('value, expected', list(AnalyticsStatus.__members__.items()))
    def test_status(self, value: str, expected: AnalyticsStatus) -> None:
        a_status = AnalyticsStatus[value]
        assert isinstance(a_status, AnalyticsStatus)
        assert a_status == expected
