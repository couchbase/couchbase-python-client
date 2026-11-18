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
KV `Get` and `Upsert` from a dict, `Query` from a script, and the health check from a list of
statuses.  Each call is recorded with its metadata and deadline, so a test can read what an impl
sent.
"""

from __future__ import annotations

import pathlib
import shutil
import socket
import subprocess  # nosec
import threading
import time
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
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2, kv_pb2_grpc
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2, query_pb2_grpc
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
                  query_pb2_grpc.QueryServiceServicer):
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
    # The serving statuses the health check answers with, in order, before it answers serving.
    health_statuses: list[int] = field(default_factory=list)
    _cas: int = 1000

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

    def Check(self, request: Any, context: grpc.ServicerContext) -> int:
        self._record('Check', request, context)
        return self.health_statuses.pop(0) if self.health_statuses else SERVING


def new_gateway() -> FakeGateway:
    """A gateway holding no documents and scripting no failures.

    The service stubs declare every servicer method abstract.  The classes protoc writes are not:
    each method `FakeGateway` leaves out answers UNIMPLEMENTED.  So it is built through its type,
    which mypy lets an abstract class be called through, rather than by name.
    """
    return cast('type[FakeGateway]', FakeGateway)()


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
