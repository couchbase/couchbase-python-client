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

from couchbase.exceptions import InvalidArgumentException
from couchbase.mutation_state import MutationState
from couchbase.n1ql import (N1QLQuery,
                            QueryProfile,
                            QueryScanConsistency)
from couchbase.options import QueryOptions
from couchbase.result import MutationToken
from couchbase.serializer import DefaultJsonSerializer


class QueryParamsUnitTests:
    # No cb_env and no TEST_MANIFEST: every test builds an N1QLQuery and reads the dict it
    # would hand the core.  Not generated either: `acouchbase.n1ql.N1QLQuery` is
    # `couchbase.logic.n1ql.N1QLQuery`, the same object rather than a subclass, and
    # `txcouchbase.n1ql` does not re-export it at all.

    Q_STR = 'SELECT * FROM default'

    @staticmethod
    def _expected(**overrides: Any) -> dict[str, Any]:
        # `metrics` is in the base because create_query_object writes it unconditionally,
        # defaulting to False.  Analytics defaults the same field to True.
        return {'statement': QueryParamsUnitTests.Q_STR, 'metrics': False, **overrides}

    @staticmethod
    def _token(bucket_name: str = 'default') -> MutationToken:
        return MutationToken(token={'partition_id': 42,
                                    'partition_uuid': 3004,
                                    'sequence_number': 3,
                                    'bucket_name': bucket_name})

    def test_consistent_with(self) -> None:
        ms = MutationState()
        mt = self._token()
        ms._add_scanvec(mt)
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(consistent_with=ms))

        # The consistent_with setter pops scan_consistency and lets the core infer AT_PLUS from
        # the presence of a mutation_state, so the encoded param is absent while the property
        # still reports AT_PLUS.  Asserting both is the point: they disagree by design.
        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt.as_dict()

        # Two tokens with identical field values collapse to one.  MutationState holds a set and
        # MutationToken hashes on all four fields, so this is deduplication by value, not by
        # identity: mt1 below is a separate object.
        ms = MutationState()
        mt1 = self._token()
        ms._add_scanvec(mt)
        ms._add_scanvec(mt1)
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(consistent_with=ms))

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt.as_dict()

        # Same four fields but a different bucket, so the hash differs and both survive.
        ms = MutationState()
        mt2 = self._token('default1')
        ms._add_scanvec(mt)
        ms._add_scanvec(mt2)
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(consistent_with=ms))

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 2
        assert next((m for m in q_mt if m == mt2.as_dict()), None) is not None

    def test_encoded_consistency(self) -> None:
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = N1QLQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.REQUEST_PLUS.value
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED)
        query = N1QLQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.NOT_BOUNDED.value
        assert query.consistency == QueryScanConsistency.NOT_BOUNDED

        # AT_PLUS is reachable only through consistent_with, which supplies the mutation_state the
        # core needs to honour it.  Naming it directly is rejected rather than silently downgraded.
        with pytest.raises(InvalidArgumentException):
            q_opts = QueryOptions(scan_consistency=QueryScanConsistency.AT_PLUS)
            N1QLQuery.create_query_object(self.Q_STR, q_opts)

    def test_params_adhoc(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(adhoc=False))
        assert query.params == self._expected(adhoc=False)

    def test_params_base(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions())
        assert query.params == self._expected()

    def test_params_client_context_id(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(client_context_id='test-string-id'))
        assert query.params == self._expected(client_context_id='test-string-id')

    def test_params_flex_index(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(flex_index=True))
        assert query.params == self._expected(flex_index=True)

    def test_params_max_parallelism(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(max_parallelism=5))
        assert query.params == self._expected(max_parallelism=5)

    def test_params_metrics(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(metrics=True))
        assert query.params == self._expected(metrics=True)

    def test_params_pipeline_batch(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(pipeline_batch=5))
        assert query.params == self._expected(pipeline_batch=5)

    def test_params_pipeline_cap(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(pipeline_cap=5))
        assert query.params == self._expected(pipeline_cap=5)

    def test_params_preserve_expiry(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(preserve_expiry=True))
        assert query.params.get('preserve_expiry', None) is True
        assert query.preserve_expiry is True

        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(preserve_expiry=False))
        assert query.params.get('preserve_expiry', None) is False
        assert query.preserve_expiry is False

        # Unset encodes nothing, and the property reports the server-side default rather than
        # None, so the two disagree.  Compare with use_replica, whose property returns None here.
        query = N1QLQuery.create_query_object(self.Q_STR)
        assert query.params.get('preserve_expiry', None) is None
        assert query.preserve_expiry is False

    def test_params_profile(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(profile=QueryProfile.PHASES))
        assert query.params == self._expected(profile=QueryProfile.PHASES.value)
        assert query.profile == QueryProfile.PHASES

    def test_params_query_context(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(query_context='bucket.scope'))
        assert query.params == self._expected(query_context='bucket.scope')

    def test_params_readonly(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(read_only=True))

        # The option is read_only and the encoded key is readonly, one word.  The rename happens
        # in the _VALID_OPTS table rather than anywhere visible from the call site.
        assert query.params == self._expected(readonly=True)

    def test_params_scan_cap(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(scan_cap=5))
        assert query.params == self._expected(scan_cap=5)

    def test_params_scan_consistency(self) -> None:
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = N1QLQuery.create_query_object(self.Q_STR, q_opts)

        assert query.params == self._expected(scan_consistency=QueryScanConsistency.REQUEST_PLUS.value)
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

    def test_params_scan_wait(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(scan_wait=timedelta(seconds=30)))
        assert query.params == self._expected(scan_wait=30000)
        # The request carries milliseconds and the property reports seconds, so the getter
        # converts rather than handing back what is stored.
        assert query.scan_wait == 30

    def test_params_scan_wait_direct(self) -> None:
        # Regression guard (PYCBC-1894): assigning the property is a public entry point in its
        # own right, reaching neither the option table that converts on the way in nor a params
        # entry to convert against, so it has to convert on the first assignment.
        query = N1QLQuery(self.Q_STR)
        query.scan_wait = timedelta(seconds=30)

        assert query.params['scan_wait'] == 30000
        assert query.scan_wait == 30

    def test_params_serializer(self) -> None:
        serializer = DefaultJsonSerializer()
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(serializer=serializer))
        assert query.params == self._expected(serializer=serializer)

    def test_params_timeout(self) -> None:
        # A timedelta, a whole number of seconds, and a fractional one, all reaching the same
        # conversion.  Held in a table annotated `Any` because the option signature declares
        # `Optional[timedelta]` while the runtime accepts a bare number too, and nothing is
        # going to reconcile the two, which would make a suppression here permanent.
        timeouts: list[tuple[Any, int]] = [(timedelta(seconds=20), 20000),
                                           (20, 20000),
                                           (25.5, 25500)]
        for timeout, expected_ms in timeouts:
            query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(timeout=timeout))
            assert query.params == self._expected(timeout=expected_ms)

    def test_params_timeout_getter(self) -> None:
        # Regression guard (PYCBC-1894): the same millisecond-to-second conversion the scan_wait
        # getter above does, and the absent case has to stay None rather than becoming zero.
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(timeout=timedelta(seconds=20)))
        assert query.timeout == 20

        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions())
        assert query.timeout is None

    def test_params_uninterpretable_enum_option(self) -> None:
        # set_option writes into params without verification, so a value neither getter can turn
        # into an enum member survives until it is read back.  Each handles None and a str and
        # has nothing left for anything else, from a signature declaring a non-optional enum.
        for option, prop in (('scan_consistency', 'consistency'), ('profile', 'profile')):
            query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions())
            query.set_option(option, 5)
            with pytest.raises(InvalidArgumentException):
                getattr(query, prop)

    def test_params_use_replica(self) -> None:
        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(use_replica=True))
        assert query.params.get('use_replica', None) is True
        assert query.use_replica is True

        query = N1QLQuery.create_query_object(self.Q_STR, QueryOptions(use_replica=False))
        assert query.params.get('use_replica', None) is False
        assert query.use_replica is False

        # Unset reports None rather than a default, because leaving the choice to the server is
        # itself meaningful here.  preserve_expiry above returns False in the same position.
        query = N1QLQuery.create_query_object(self.Q_STR)
        assert query.params.get('use_replica', None) is None
        assert query.use_replica is None
