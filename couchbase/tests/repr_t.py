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

import datetime
from collections.abc import Callable
from typing import Any

import pytest

import couchbase.encryption
import couchbase.kv_range_scan
import couchbase.management.buckets
import couchbase.management.collections
import couchbase.management.users
import couchbase.management.views
import couchbase.options
import couchbase.search
import couchbase.search_scoring
import couchbase.subdocument
import couchbase.transactions
import couchbase.vector_search
from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.common.transactions.transaction_query_results import TransactionQueryResults
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.durability import ClientDurability
from couchbase.encryption import EncryptionResult, Key
from couchbase.exceptions import TransactionsErrorContext
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.management.analytics import (AzureBlobExternalAnalyticsLink,
                                            CouchbaseAnalyticsEncryptionSettings,
                                            CouchbaseRemoteAnalyticsLink,
                                            S3ExternalAnalyticsLink)
from couchbase.management.buckets import BucketSettings, CreateBucketSettings
from couchbase.management.collections import (CollectionSpec,
                                              CreateCollectionSettings,
                                              ScopeSpec,
                                              UpdateCollectionSettings)
from couchbase.management.eventing import (EventingFunctionUrlAuthBasic,
                                           EventingFunctionUrlAuthBearer,
                                           EventingFunctionUrlAuthDigest)
from couchbase.management.users import (Group,
                                        Origin,
                                        Role,
                                        RoleAndDescription,
                                        RoleAndOrigins,
                                        User)
from couchbase.management.views import DesignDocument, View
from couchbase.options import ClusterOptions, UnsignedInt64
from couchbase.result import ValueResult
from couchbase.search import SearchRequest
from couchbase.search_scoring import ReciprocalRankFusion
from couchbase.subdocument import ArrayValues, MutationMacro
from couchbase.transactions import TransactionKeyspace
from couchbase.vector_search import VectorQuery, VectorSearch

SECRET = 's3cr3t'

EXPIRY = datetime.timedelta(seconds=5)

# The names a repr may use: every public name of the modules the round-tripped classes come from.
_NAMESPACE: dict[str, Any] = {'datetime': datetime}
for _module in (couchbase.encryption, couchbase.kv_range_scan, couchbase.management.buckets,
                couchbase.management.collections, couchbase.management.users, couchbase.management.views,
                couchbase.options, couchbase.search, couchbase.search_scoring, couchbase.subdocument,
                couchbase.transactions, couchbase.vector_search):
    _NAMESPACE.update({name: getattr(_module, name) for name in _module.__all__})


class ReprUnitTests:

    @pytest.mark.parametrize('build', [
        lambda: PasswordAuthenticator('user', SECRET),
        lambda: JwtAuthenticator(SECRET),
        lambda: CertificateAuthenticator(cert_path='/certs/client.pem', key_path=SECRET),
        lambda: ClusterOptions(PasswordAuthenticator('user', SECRET)),
        lambda: User('user', password=SECRET),
        lambda: S3ExternalAnalyticsLink('dv', 'link', 'AKIA', 'us-east-1',
                                        secret_access_key=SECRET, session_token=SECRET),
        lambda: AzureBlobExternalAnalyticsLink('dv', 'link', connection_string=SECRET, account_key=SECRET,
                                               shared_access_signature=SECRET),
        lambda: CouchbaseRemoteAnalyticsLink('dv', 'link', 'host',
                                             CouchbaseAnalyticsEncryptionSettings(client_key=SECRET.encode()),
                                             username='user', password=SECRET),
        lambda: Key('key-1', SECRET.encode()),
        lambda: EventingFunctionUrlAuthBasic('user', SECRET),
        lambda: EventingFunctionUrlAuthDigest('user', SECRET),
        lambda: EventingFunctionUrlAuthBearer(SECRET),
    ])
    def test_a_secret_is_redacted(self, build: Callable[[], Any]) -> None:
        text = repr(build())

        assert SECRET not in text
        assert '<redacted>' in text
        assert str(build()) == text

    def test_an_unset_secret_reads_none(self) -> None:
        assert repr(User('user')).endswith('password=None)')

    @pytest.mark.parametrize('build, expected', [
        (lambda: TransactionsErrorContext(failure_type='FAIL_OTHER'),
         "TransactionsErrorContext({'failure_type': 'FAIL_OTHER'})"),
        (lambda: TransactionQueryResults(b'{"results": []}'),
         "TransactionQueryResults(res={'results': []})"),
    ])
    def test_a_class_prints_its_own_name(self, build: Callable[[], Any], expected: str) -> None:
        assert repr(build()) == expected

    def test_value_result_prints_its_own_name(self) -> None:
        with pytest.warns(CouchbaseDeprecationWarning):
            result = ValueResult(11, None, 'value')

        assert repr(result) == 'ValueResult(mutation_token=None, cas=11)'

    def test_a_subclass_prints_its_own_name(self) -> None:
        class _Durability(ClientDurability):
            pass

        assert repr(_Durability()).startswith('_Durability(')

    @pytest.mark.parametrize('build', [
        lambda: ArrayValues(1, 'a'),
        lambda: BucketSettings(name='b', ram_quota_mb=100, max_expiry=EXPIRY),
        lambda: CollectionSpec('c', 's', max_expiry=EXPIRY),
        lambda: CreateBucketSettings(name='b', num_replicas=1),
        lambda: CreateCollectionSettings(max_expiry=EXPIRY, history=True),
        lambda: DesignDocument('dd', {'v': View('function(doc){}', reduce='_count')}),
        lambda: EncryptionResult('alg', 'kid', 'YWJj'),
        lambda: Group('g', description='d', roles=[Role('admin')]),
        lambda: MutationMacro.cas(),
        lambda: PrefixScan('p'),
        lambda: RangeScan(ScanTerm('a'), ScanTerm('z', exclusive=True)),
        lambda: ReciprocalRankFusion(rank_constant=60, window_size=5),
        lambda: RoleAndDescription(Role('admin'), 'Admin', 'Full access', True),
        lambda: RoleAndOrigins(Role('admin', bucket='b'), [Origin('user')]),
        lambda: SamplingScan(10, seed=1),
        lambda: ScopeSpec('s', [CollectionSpec('c', 's')]),
        lambda: SearchRequest(VectorSearch([VectorQuery('f', [0.1, 0.2], num_candidates=3)])),
        lambda: TransactionKeyspace(bucket='b', scope='s', collection='c'),
        lambda: UnsignedInt64(5),
        lambda: UpdateCollectionSettings(max_expiry=EXPIRY),
        lambda: User('u', display_name='U', roles=[Role('admin')]),
    ])
    def test_a_repr_rebuilds_the_object(self, build: Callable[[], Any]) -> None:
        text = repr(build())

        assert repr(eval(text, _NAMESPACE)) == text
