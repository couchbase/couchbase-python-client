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

"""An in-process stand-in for the gateway, so the couchbase2 impls can be run with no cluster.

It serves TLS, as the gateway does, on a certificate signed by a CA made for the run, and answers
KV `Get` and `Upsert` from a dict, `Query` and `SearchQuery` each from a script, the collection admin
service from a manifest, the bucket admin service from a dict of buckets, the query and search admin
services each from a dict of indexes, and the health check from a list of statuses.  Each call is
recorded with its metadata and deadline, so a test can read what an impl sent.
"""

from __future__ import annotations

import pathlib
import shutil
import socket
import subprocess  # nosec
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, cast

import grpc
from google.protobuf import any_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc import error_details_pb2, status_pb2

from couchbase.protocol.couchbase2._error_mapping import STATUS_DETAILS_KEY
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2, bucket_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2, collection_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2 as query_admin_pb2
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2_grpc as query_admin_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2, search_pb2_grpc
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2, kv_pb2_grpc
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2, query_pb2_grpc
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2 as search_query_pb2
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2_grpc as search_query_pb2_grpc
from couchbase.protocol.couchbase2._health import (SERVING,
                                                   HealthCheckRequest,
                                                   HealthCheckResponse)


@dataclass(frozen=True)
class Certificates:
    ca: pathlib.Path
    certificate: pathlib.Path
    key: pathlib.Path


def make_certificates(directory: pathlib.Path) -> Certificates | None:
    """A CA and a `localhost` certificate it signed, or None when `openssl` is not on PATH."""
    if shutil.which('openssl') is None:
        return None

    def openssl(*args: str) -> None:
        done = subprocess.run(['openssl', *args], cwd=directory, capture_output=True, text=True)  # nosec B603 B607
        if done.returncode:
            raise RuntimeError(f'openssl {args[0]} exited {done.returncode}: {done.stderr.strip()}')

    # `req` always reads a config file.  A platform's own may be missing, as on Windows, or may add
    # extensions of its own that clash with the ones asked for, so every `req` names this one.
    (directory / 'req.cnf').write_text('[req]\ndistinguished_name = dn\n[dn]\n[ca]\n'
                                       'basicConstraints = critical, CA:TRUE\n'
                                       'keyUsage = critical, keyCertSign, cRLSign\n'
                                       'subjectKeyIdentifier = hash\n')
    openssl('req', '-config', 'req.cnf', '-x509', '-extensions', 'ca', '-newkey', 'rsa:2048', '-nodes',
            '-keyout', 'ca.key', '-out', 'ca.crt', '-days', '2', '-subj', '/CN=test-ca')
    (directory / 'localhost.ext').write_text('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n')
    openssl('req', '-config', 'req.cnf', '-newkey', 'rsa:2048', '-nodes', '-keyout', 'localhost.key',
            '-out', 'localhost.csr', '-subj', '/CN=localhost')
    openssl('x509', '-req', '-in', 'localhost.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key', '-CAcreateserial',
            '-out', 'localhost.crt', '-days', '2', '-extfile', 'localhost.ext')
    return Certificates(directory / 'ca.crt', directory / 'localhost.crt', directory / 'localhost.key')


@dataclass
class Call:
    method: str
    request: Any
    metadata: dict[str, Any]
    # The seconds left before the call's deadline when it arrived, by gRPC's own clock.  A bound on
    # the time a call was given reads this alone: before Python 3.13 `time.monotonic()` ticks every
    # 15.6 ms on Windows, so a bound read across the two clocks can overshoot.
    remaining: float
    # When the call arrived, by `time.monotonic()`.
    arrived: float

    @property
    def deadline(self) -> float:
        """When the call's deadline falls, by `time.monotonic()`."""
        return self.arrived + self.remaining


@dataclass
class Document:
    content: bytes
    flags: int
    cas: int
    expiry: int | None = None


@dataclass(frozen=True)
class Fail:
    code: grpc.StatusCode
    resource_type: str | None = None
    violation: str | None = None


@dataclass
class FakeGateway(kv_pb2_grpc.KvServiceServicer,
                  query_pb2_grpc.QueryServiceServicer,
                  search_query_pb2_grpc.SearchServiceServicer,
                  collection_pb2_grpc.CollectionAdminServiceServicer,
                  bucket_pb2_grpc.BucketAdminServiceServicer):
    documents: dict[tuple[str, str, str, str], Document] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)
    # Failures each method answers with, in order, before it answers normally.
    failures: dict[str, list[Fail]] = field(default_factory=dict)
    # The messages a query streams, then how it ends: OK when None.
    query_messages: Sequence[query_pb2.QueryResponse] = ()
    query_end: Fail | None = None
    # Set when a query stream left open is cancelled by the client.
    query_cancelled: threading.Event = field(default_factory=threading.Event)
    # How long a query waits after each message it sends.
    query_pause: float = 0.0
    # The messages a search streams, then how it ends: OK when None.
    search_messages: Sequence[search_query_pb2.SearchQueryResponse] = ()
    search_end: Fail | None = None
    # Each bucket's scopes, each scope's collections by name.
    manifest: dict[str, dict[str, dict[str, collection_pb2.ListCollectionsResponse.Collection]]] = field(
        default_factory=dict)
    # The buckets by name, each as the listing reports it.
    buckets: dict[str, bucket_pb2.ListBucketsResponse.Bucket] = field(default_factory=dict)
    # The serving statuses the health check answers with, in order, before it answers serving.
    health_statuses: list[int] = field(default_factory=list)
    # The query admin service, which records its calls here.
    query_admin: FakeQueryAdmin = field(init=False)
    # The search admin service, which records its calls here.
    search_admin: FakeSearchAdmin = field(init=False)
    _cas: int = 1000

    def __post_init__(self) -> None:
        self.query_admin = cast('type[FakeQueryAdmin]', FakeQueryAdmin)(self)
        self.search_admin = cast('type[FakeSearchAdmin]', FakeSearchAdmin)(self)

    def _record(self, method: str, request: Any, context: grpc.ServicerContext) -> None:
        remaining = context.time_remaining()
        self.calls.append(Call(method, request, dict(context.invocation_metadata()),
                               remaining if remaining is not None else 0, time.monotonic()))
        pending = self.failures.get(method)
        if pending:
            _abort(context, pending.pop(0))

    def Get(self, request: kv_pb2.GetRequest, context: grpc.ServicerContext) -> kv_pb2.GetResponse:
        self._record('Get', request, context)
        document = self.documents.get((request.bucket_name, request.scope_name, request.collection_name, request.key))
        if document is None:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'document'))
            raise AssertionError('unreachable')
        response = kv_pb2.GetResponse(content_uncompressed=document.content, content_flags=document.flags,
                                      cas=document.cas)
        response.expiry.CopyFrom(Timestamp(seconds=document.expiry or 0))
        return response

    def Upsert(self, request: kv_pb2.UpsertRequest, context: grpc.ServicerContext) -> kv_pb2.UpsertResponse:
        self._record('Upsert', request, context)
        self._cas += 1
        key = (request.bucket_name, request.scope_name, request.collection_name, request.key)
        self.documents[key] = Document(request.content_uncompressed, request.content_flags, self._cas,
                                       request.expiry_secs or None)
        token = kv_pb2.MutationToken(bucket_name=request.bucket_name, vbucket_id=7, vbucket_uuid=11,
                                     seq_no=self._cas)
        return kv_pb2.UpsertResponse(cas=self._cas, mutation_token=token)

    def Query(self, request: query_pb2.QueryRequest, context: grpc.ServicerContext
              ) -> Iterator[query_pb2.QueryResponse]:
        self._record('Query', request, context)
        for message in self.query_messages:
            yield message
            if self.query_pause:
                deadline = time.monotonic() + self.query_pause
                while context.is_active() and time.monotonic() < deadline:
                    time.sleep(0.01)
                if not context.is_active():
                    self.query_cancelled.set()
                    return
        if self.query_end is not None:
            _abort(context, self.query_end)

    def SearchQuery(self, request: search_query_pb2.SearchQueryRequest, context: grpc.ServicerContext
                    ) -> Iterator[search_query_pb2.SearchQueryResponse]:
        self._record('SearchQuery', request, context)
        yield from self.search_messages
        if self.search_end is not None:
            _abort(context, self.search_end)

    def Check(self, request: Any, context: grpc.ServicerContext) -> int:
        self._record('Check', request, context)
        return self.health_statuses.pop(0) if self.health_statuses else SERVING

    def ListCollections(self, request: collection_pb2.ListCollectionsRequest, context: grpc.ServicerContext
                        ) -> collection_pb2.ListCollectionsResponse:
        self._record('ListCollections', request, context)
        scopes = self._bucket(request.bucket_name, context)
        return collection_pb2.ListCollectionsResponse(
            scopes=[collection_pb2.ListCollectionsResponse.Scope(name=name, collections=list(collections.values()))
                    for name, collections in scopes.items()])

    def CreateScope(self, request: collection_pb2.CreateScopeRequest, context: grpc.ServicerContext
                    ) -> collection_pb2.CreateScopeResponse:
        self._record('CreateScope', request, context)
        scopes = self._bucket(request.bucket_name, context)
        if request.scope_name in scopes:
            _abort(context, Fail(grpc.StatusCode.ALREADY_EXISTS, 'scope'))
        scopes[request.scope_name] = {}
        return collection_pb2.CreateScopeResponse()

    def DeleteScope(self, request: collection_pb2.DeleteScopeRequest, context: grpc.ServicerContext
                    ) -> collection_pb2.DeleteScopeResponse:
        self._record('DeleteScope', request, context)
        self._scope(request.bucket_name, request.scope_name, context)
        del self.manifest[request.bucket_name][request.scope_name]
        return collection_pb2.DeleteScopeResponse()

    def CreateCollection(self, request: collection_pb2.CreateCollectionRequest, context: grpc.ServicerContext
                         ) -> collection_pb2.CreateCollectionResponse:
        self._record('CreateCollection', request, context)
        collections = self._scope(request.bucket_name, request.scope_name, context)
        if request.collection_name in collections:
            _abort(context, Fail(grpc.StatusCode.ALREADY_EXISTS, 'collection'))
        collection = collection_pb2.ListCollectionsResponse.Collection(name=request.collection_name)
        if request.HasField('max_expiry_secs'):
            collection.max_expiry_secs = request.max_expiry_secs
        if request.HasField('history_retention_enabled'):
            collection.history_retention_enabled = request.history_retention_enabled
        collections[request.collection_name] = collection
        return collection_pb2.CreateCollectionResponse()

    def UpdateCollection(self, request: collection_pb2.UpdateCollectionRequest, context: grpc.ServicerContext
                         ) -> collection_pb2.UpdateCollectionResponse:
        self._record('UpdateCollection', request, context)
        collection = self._collection(request.bucket_name, request.scope_name, request.collection_name, context)
        if request.HasField('max_expiry_secs'):
            collection.max_expiry_secs = request.max_expiry_secs
        if request.HasField('history_retention_enabled'):
            collection.history_retention_enabled = request.history_retention_enabled
        return collection_pb2.UpdateCollectionResponse()

    def DeleteCollection(self, request: collection_pb2.DeleteCollectionRequest, context: grpc.ServicerContext
                         ) -> collection_pb2.DeleteCollectionResponse:
        self._record('DeleteCollection', request, context)
        self._collection(request.bucket_name, request.scope_name, request.collection_name, context)
        del self.manifest[request.bucket_name][request.scope_name][request.collection_name]
        return collection_pb2.DeleteCollectionResponse()

    def ListBuckets(self, request: bucket_pb2.ListBucketsRequest, context: grpc.ServicerContext
                    ) -> bucket_pb2.ListBucketsResponse:
        self._record('ListBuckets', request, context)
        return bucket_pb2.ListBucketsResponse(buckets=list(self.buckets.values()))

    def CreateBucket(self, request: bucket_pb2.CreateBucketRequest, context: grpc.ServicerContext
                     ) -> bucket_pb2.CreateBucketResponse:
        self._record('CreateBucket', request, context)
        if request.bucket_name in self.buckets:
            _abort(context, Fail(grpc.StatusCode.ALREADY_EXISTS, 'bucket'))
        self.buckets[request.bucket_name] = _copied(request, bucket_pb2.ListBucketsResponse.Bucket())
        return bucket_pb2.CreateBucketResponse(bucket_uuid='uuid')

    def UpdateBucket(self, request: bucket_pb2.UpdateBucketRequest, context: grpc.ServicerContext
                     ) -> bucket_pb2.UpdateBucketResponse:
        self._record('UpdateBucket', request, context)
        _copied(request, self._listed_bucket(request.bucket_name, context))
        return bucket_pb2.UpdateBucketResponse()

    def DeleteBucket(self, request: bucket_pb2.DeleteBucketRequest, context: grpc.ServicerContext
                     ) -> bucket_pb2.DeleteBucketResponse:
        self._record('DeleteBucket', request, context)
        self._listed_bucket(request.bucket_name, context)
        del self.buckets[request.bucket_name]
        return bucket_pb2.DeleteBucketResponse()

    def FlushBucket(self, request: bucket_pb2.FlushBucketRequest, context: grpc.ServicerContext
                    ) -> bucket_pb2.FlushBucketResponse:
        self._record('FlushBucket', request, context)
        self._listed_bucket(request.bucket_name, context)
        return bucket_pb2.FlushBucketResponse()

    def _listed_bucket(self, bucket_name: str, context: grpc.ServicerContext
                       ) -> bucket_pb2.ListBucketsResponse.Bucket:
        if bucket_name not in self.buckets:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'bucket'))
        return self.buckets[bucket_name]

    def _bucket(self, bucket_name: str, context: grpc.ServicerContext
                ) -> dict[str, dict[str, collection_pb2.ListCollectionsResponse.Collection]]:
        if bucket_name not in self.manifest:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'bucket'))
        return self.manifest[bucket_name]

    def _scope(self, bucket_name: str, scope_name: str, context: grpc.ServicerContext
               ) -> dict[str, collection_pb2.ListCollectionsResponse.Collection]:
        scopes = self._bucket(bucket_name, context)
        if scope_name not in scopes:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'scope'))
        return scopes[scope_name]

    def _collection(self, bucket_name: str, scope_name: str, collection_name: str, context: grpc.ServicerContext
                    ) -> collection_pb2.ListCollectionsResponse.Collection:
        collections = self._scope(bucket_name, scope_name, context)
        if collection_name not in collections:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'collection'))
        return collections[collection_name]


Index = query_admin_pb2.GetAllIndexesResponse.Index


@dataclass
class FakeQueryAdmin(query_admin_pb2_grpc.QueryAdminServiceServicer):
    """The query admin service, apart from `FakeGateway` because the search admin service has a
    `CreateIndex` of its own.

    An index without a scope or collection is on the default collection, as the gateway reads it.
    `BuildDeferredIndexes` leaves each index it builds building, and the listing or wait after brings
    it online.
    """

    gateway: FakeGateway
    # The indexes by bucket, scope, collection and name.
    indexes: dict[tuple[str, str, str, str], Index] = field(default_factory=dict)

    def GetAllIndexes(self, request: query_admin_pb2.GetAllIndexesRequest, context: grpc.ServicerContext
                      ) -> query_admin_pb2.GetAllIndexesResponse:
        self.gateway._record('GetAllIndexes', request, context)
        listed = [index for key, index in self.indexes.items() if _in_keyspace(key, request)]
        response = query_admin_pb2.GetAllIndexesResponse(indexes=listed)
        for index in listed:
            if index.state == query_admin_pb2.INDEX_STATE_BUILDING:
                index.state = query_admin_pb2.INDEX_STATE_ONLINE
        return response

    def WaitForIndexOnline(self, request: query_admin_pb2.WaitForIndexOnlineRequest, context: grpc.ServicerContext
                           ) -> query_admin_pb2.WaitForIndexOnlineResponse:
        """Refuses a deferred index, as the gateway does, rather than waiting for it to be built."""
        self.gateway._record('WaitForIndexOnline', request, context)
        key = (request.bucket_name, request.scope_name or '_default', request.collection_name or '_default',
               request.name)
        index = self.indexes.get(key)
        if index is None:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'queryindex'))
        elif index.state == query_admin_pb2.INDEX_STATE_DEFERRED:
            _abort(context, Fail(grpc.StatusCode.FAILED_PRECONDITION, violation='NOT_BUILDING'))
        else:
            index.state = query_admin_pb2.INDEX_STATE_ONLINE
        return query_admin_pb2.WaitForIndexOnlineResponse()

    def CreateIndex(self, request: query_admin_pb2.CreateIndexRequest, context: grpc.ServicerContext
                    ) -> query_admin_pb2.CreateIndexResponse:
        self.gateway._record('CreateIndex', request, context)
        self._create(request, request.name, Index(fields=request.fields), context)
        return query_admin_pb2.CreateIndexResponse()

    def CreatePrimaryIndex(self, request: query_admin_pb2.CreatePrimaryIndexRequest, context: grpc.ServicerContext
                           ) -> query_admin_pb2.CreatePrimaryIndexResponse:
        self.gateway._record('CreatePrimaryIndex', request, context)
        name = request.name if request.HasField('name') else '#primary'
        self._create(request, name, Index(is_primary=True), context)
        return query_admin_pb2.CreatePrimaryIndexResponse()

    def DropIndex(self, request: query_admin_pb2.DropIndexRequest, context: grpc.ServicerContext
                  ) -> query_admin_pb2.DropIndexResponse:
        self.gateway._record('DropIndex', request, context)
        self._drop(request, request.name, context)
        return query_admin_pb2.DropIndexResponse()

    def DropPrimaryIndex(self, request: query_admin_pb2.DropPrimaryIndexRequest, context: grpc.ServicerContext
                         ) -> query_admin_pb2.DropPrimaryIndexResponse:
        self.gateway._record('DropPrimaryIndex', request, context)
        self._drop(request, request.name if request.HasField('name') else '#primary', context)
        return query_admin_pb2.DropPrimaryIndexResponse()

    def BuildDeferredIndexes(self, request: query_admin_pb2.BuildDeferredIndexesRequest,
                             context: grpc.ServicerContext) -> query_admin_pb2.BuildDeferredIndexesResponse:
        self.gateway._record('BuildDeferredIndexes', request, context)
        built = []
        for key, index in self.indexes.items():
            if _in_keyspace(key, request) and index.state == query_admin_pb2.INDEX_STATE_DEFERRED:
                index.state = query_admin_pb2.INDEX_STATE_BUILDING
                built.append(query_admin_pb2.BuildDeferredIndexesResponse.Index(name=index.name))
        return query_admin_pb2.BuildDeferredIndexesResponse(indexes=built)

    def _create(self, request: Any, name: str, index: Index, context: grpc.ServicerContext) -> None:
        key = (request.bucket_name, *_keyspace(request), name)
        if key in self.indexes:
            if request.ignore_if_exists:
                return
            _abort(context, Fail(grpc.StatusCode.ALREADY_EXISTS, 'queryindex'))
        index.bucket_name, index.scope_name, index.collection_name, index.name = key
        index.type = query_admin_pb2.INDEX_TYPE_GSI
        index.state = query_admin_pb2.INDEX_STATE_DEFERRED if request.deferred else query_admin_pb2.INDEX_STATE_ONLINE
        self.indexes[key] = index

    def _drop(self, request: Any, name: str, context: grpc.ServicerContext) -> None:
        key = (request.bucket_name, *_keyspace(request), name)
        if key not in self.indexes:
            if request.ignore_if_missing:
                return
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'queryindex'))
        del self.indexes[key]


@dataclass
class FakeSearchAdmin(search_pb2_grpc.SearchAdminServiceServicer):
    """The search admin service.  An index without a bucket and scope is the cluster's own.

    Each index is held as the gateway sends it, so a test can plant a definition the SDK cannot read.
    The count and the analysis are canned, and the analysis reports no status.
    """

    gateway: FakeGateway
    # The indexes by bucket, scope and name.
    indexes: dict[tuple[str, str, str], search_pb2.Index] = field(default_factory=dict)
    # The switches each control call has flipped, by index key: `ingest`, `querying` and `plan`.
    controls: dict[tuple[str, str, str], dict[str, bool]] = field(default_factory=dict)

    def CreateIndex(self, request: search_pb2.CreateIndexRequest, context: grpc.ServicerContext
                    ) -> search_pb2.CreateIndexResponse:
        self.gateway._record('CreateIndex', request, context)
        key = (request.bucket_name, request.scope_name, request.name)
        if key in self.indexes:
            _abort(context, Fail(grpc.StatusCode.ALREADY_EXISTS, 'searchindex'))
        index = search_pb2.Index(uuid=uuid.uuid4().hex)
        for descriptor, value in request.ListFields():
            if descriptor.name in ('params', 'plan_params', 'source_params'):
                getattr(index, descriptor.name).update(value)
            elif descriptor.name not in ('bucket_name', 'scope_name', 'prev_index_uuid'):
                setattr(index, descriptor.name, value)
        self.indexes[key] = index
        return search_pb2.CreateIndexResponse()

    def UpdateIndex(self, request: search_pb2.UpdateIndexRequest, context: grpc.ServicerContext
                    ) -> search_pb2.UpdateIndexResponse:
        self.gateway._record('UpdateIndex', request, context)
        key = (request.bucket_name, request.scope_name, request.index.name)
        if not request.index.uuid:
            _abort(context, Fail(grpc.StatusCode.INVALID_ARGUMENT))
        if self._index(key, context).uuid != request.index.uuid:
            _abort(context, Fail(grpc.StatusCode.ABORTED))
        updated = search_pb2.Index()
        updated.CopyFrom(request.index)
        updated.uuid = uuid.uuid4().hex
        self.indexes[key] = updated
        return search_pb2.UpdateIndexResponse()

    def GetIndex(self, request: search_pb2.GetIndexRequest, context: grpc.ServicerContext
                 ) -> search_pb2.GetIndexResponse:
        self.gateway._record('GetIndex', request, context)
        return search_pb2.GetIndexResponse(index=self._index(_search_key(request), context))

    def ListIndexes(self, request: search_pb2.ListIndexesRequest, context: grpc.ServicerContext
                    ) -> search_pb2.ListIndexesResponse:
        self.gateway._record('ListIndexes', request, context)
        listed = [index for (bucket_name, scope_name, _), index in self.indexes.items()
                  if (bucket_name, scope_name) == (request.bucket_name, request.scope_name)]
        return search_pb2.ListIndexesResponse(indexes=listed)

    def DeleteIndex(self, request: search_pb2.DeleteIndexRequest, context: grpc.ServicerContext
                    ) -> search_pb2.DeleteIndexResponse:
        self.gateway._record('DeleteIndex', request, context)
        self._index(_search_key(request), context)
        del self.indexes[_search_key(request)]
        return search_pb2.DeleteIndexResponse()

    def AnalyzeDocument(self, request: search_pb2.AnalyzeDocumentRequest, context: grpc.ServicerContext
                        ) -> search_pb2.AnalyzeDocumentResponse:
        self.gateway._record('AnalyzeDocument', request, context)
        self._index(_search_key(request), context)
        return search_pb2.AnalyzeDocumentResponse(analyzed=b'[{"doc": ' + request.doc + b'}]')

    def GetIndexedDocumentsCount(self, request: search_pb2.GetIndexedDocumentsCountRequest,
                                 context: grpc.ServicerContext) -> search_pb2.GetIndexedDocumentsCountResponse:
        self.gateway._record('GetIndexedDocumentsCount', request, context)
        self._index(_search_key(request), context)
        return search_pb2.GetIndexedDocumentsCountResponse(count=7)

    def PauseIndexIngest(self, request: search_pb2.PauseIndexIngestRequest, context: grpc.ServicerContext
                         ) -> search_pb2.PauseIndexIngestResponse:
        self._control('PauseIndexIngest', request, context, 'ingest', False)
        return search_pb2.PauseIndexIngestResponse()

    def ResumeIndexIngest(self, request: search_pb2.ResumeIndexIngestRequest, context: grpc.ServicerContext
                          ) -> search_pb2.ResumeIndexIngestResponse:
        self._control('ResumeIndexIngest', request, context, 'ingest', True)
        return search_pb2.ResumeIndexIngestResponse()

    def AllowIndexQuerying(self, request: search_pb2.AllowIndexQueryingRequest, context: grpc.ServicerContext
                           ) -> search_pb2.AllowIndexQueryingResponse:
        self._control('AllowIndexQuerying', request, context, 'querying', True)
        return search_pb2.AllowIndexQueryingResponse()

    def DisallowIndexQuerying(self, request: search_pb2.DisallowIndexQueryingRequest, context: grpc.ServicerContext
                              ) -> search_pb2.DisallowIndexQueryingResponse:
        self._control('DisallowIndexQuerying', request, context, 'querying', False)
        return search_pb2.DisallowIndexQueryingResponse()

    def FreezeIndexPlan(self, request: search_pb2.FreezeIndexPlanRequest, context: grpc.ServicerContext
                        ) -> search_pb2.FreezeIndexPlanResponse:
        self._control('FreezeIndexPlan', request, context, 'plan', False)
        return search_pb2.FreezeIndexPlanResponse()

    def UnfreezeIndexPlan(self, request: search_pb2.UnfreezeIndexPlanRequest, context: grpc.ServicerContext
                          ) -> search_pb2.UnfreezeIndexPlanResponse:
        self._control('UnfreezeIndexPlan', request, context, 'plan', True)
        return search_pb2.UnfreezeIndexPlanResponse()

    def _control(self, method: str, request: Any, context: grpc.ServicerContext, switch: str, on: bool) -> None:
        self.gateway._record(method, request, context)
        key = _search_key(request)
        self._index(key, context)
        self.controls.setdefault(key, {})[switch] = on

    def _index(self, key: tuple[str, str, str], context: grpc.ServicerContext) -> search_pb2.Index:
        if key not in self.indexes:
            _abort(context, Fail(grpc.StatusCode.NOT_FOUND, 'searchindex'))
        return self.indexes[key]


def _search_key(request: Any) -> tuple[str, str, str]:
    return request.bucket_name, request.scope_name, request.name


def _keyspace(request: Any) -> tuple[str, str]:
    return (request.scope_name if request.HasField('scope_name') else '_default',
            request.collection_name if request.HasField('collection_name') else '_default')


def _in_keyspace(key: tuple[str, str, str, str], request: Any) -> bool:
    bucket_name, scope_name, collection_name, _ = key
    # Only the listing may leave its bucket out, and it then lists every bucket.
    if request.bucket_name and request.bucket_name != bucket_name:
        return False
    if request.HasField('scope_name') and request.scope_name != scope_name:
        return False
    return not request.HasField('collection_name') or request.collection_name == collection_name


def new_gateway() -> FakeGateway:
    """A gateway holding no documents and scripting no failures.

    The service stubs declare every servicer method abstract.  The classes protoc writes are not:
    each method `FakeGateway` leaves out answers UNIMPLEMENTED.  So it is built through its type,
    which mypy lets an abstract class be called through, rather than by name.
    """
    return cast('type[FakeGateway]', FakeGateway)()


def _copied(request: Any, bucket: bucket_pb2.ListBucketsResponse.Bucket) -> bucket_pb2.ListBucketsResponse.Bucket:
    """`bucket` with each field `request` sets, which the two spell alike."""
    for descriptor, value in request.ListFields():
        setattr(bucket, descriptor.name, value)
    return bucket


def _abort(context: grpc.ServicerContext, fail: Fail) -> None:
    detail = any_pb2.Any()
    if fail.resource_type is not None:
        detail.Pack(error_details_pb2.ResourceInfo(resource_type=fail.resource_type, resource_name='r'))
    elif fail.violation is not None:
        violation = error_details_pb2.PreconditionFailure.Violation(type=fail.violation)
        detail.Pack(error_details_pb2.PreconditionFailure(violations=[violation]))
    if detail.type_url:
        rich = status_pb2.Status(code=fail.code.value[0], message='scripted', details=[detail])
        context.set_trailing_metadata(((STATUS_DETAILS_KEY, rich.SerializeToString()),))
    context.abort(fail.code, 'scripted')


@contextmanager
def serving(gateway: FakeGateway, certificates: Certificates) -> Iterator[int]:
    """Serve `gateway` on a free port of localhost, answering on the port it yields."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kv_pb2_grpc.add_KvServiceServicer_to_server(gateway, server)
    query_pb2_grpc.add_QueryServiceServicer_to_server(gateway, server)
    search_query_pb2_grpc.add_SearchServiceServicer_to_server(gateway, server)
    collection_pb2_grpc.add_CollectionAdminServiceServicer_to_server(gateway, server)
    bucket_pb2_grpc.add_BucketAdminServiceServicer_to_server(gateway, server)
    query_admin_pb2_grpc.add_QueryAdminServiceServicer_to_server(gateway.query_admin, server)
    search_pb2_grpc.add_SearchAdminServiceServicer_to_server(gateway.search_admin, server)
    check: grpc.RpcMethodHandler[Any, int] = grpc.unary_unary_rpc_method_handler(
        gateway.Check,
        request_deserializer=HealthCheckRequest.FromString,
        response_serializer=lambda status: HealthCheckResponse(status=status).SerializeToString())
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('grpc.health.v1.Health',
                                                                          {'Check': check}),))
    credentials = grpc.ssl_server_credentials([(certificates.key.read_bytes(),
                                                certificates.certificate.read_bytes())])
    port = server.add_secure_port('localhost:0', credentials)
    server.start()
    try:
        yield port
    finally:
        server.stop(None)


@contextmanager
def dropping() -> Iterator[int]:
    """Accept each connection on a free port of 127.0.0.1 and close it at once, yielding the port.

    A client finds the gateway unavailable on every attempt, and finds it at once.  A closed port
    does not do that everywhere: Windows retries a refused connection before reporting it, which
    outlasts a short timeout.
    """
    listener = socket.create_server(('127.0.0.1', 0))
    listener.settimeout(0.05)
    closing = threading.Event()

    def drop() -> None:
        while not closing.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            connection.close()

    thread = threading.Thread(target=drop, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1]
    finally:
        closing.set()
        thread.join()
        listener.close()
