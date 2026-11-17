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

"""`wait_until_ready` over the gateway: the standard gRPC health check, repeated until it reports serving.

RFC 77 asks for `grpc.health.v1.Health/Check` with `service` unset, retried by the usual rules, and
nothing more.  The gateway answers it for itself without looking at the cluster behind it, so a
serving answer says the gateway can be reached and no more: not that the cluster is ready, nor that
the credentials are good, since the check carries no `authorization` metadata.

The two messages are described here, in a descriptor pool of their own, rather than generated.
`grpcio-health-checking` registers the same file in the default pool, so generating it would break
an application that imports both, and depending on that package would tie the `protobuf` and
`grpcio` a user may install to its own release.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, cast

import grpc
import grpc.aio
from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClass

from couchbase.common.exceptions import CouchbaseException
from couchbase.protocol.couchbase2._driver import call_unary, call_unary_async
from couchbase.protocol.couchbase2._error_mapping import MappedError
from couchbase.protocol.couchbase2._request_context import RequestContext
from couchbase.protocol.couchbase2._retry import RetryReason

CHECK = '/grpc.health.v1.Health/Check'


def _health_file() -> descriptor_pb2.FileDescriptorProto:
    field = descriptor_pb2.FieldDescriptorProto
    file = descriptor_pb2.FileDescriptorProto(name='grpc/health/v1/health.proto',
                                              package='grpc.health.v1',
                                              syntax='proto3')
    file.message_type.add(name='HealthCheckRequest').field.add(
        name='service', number=1, type=field.TYPE_STRING, label=field.LABEL_OPTIONAL)
    response = file.message_type.add(name='HealthCheckResponse')
    status = response.enum_type.add(name='ServingStatus')
    for number, name in enumerate(('UNKNOWN', 'SERVING', 'NOT_SERVING', 'SERVICE_UNKNOWN')):
        status.value.add(name=name, number=number)
    response.field.add(name='status', number=1, type=field.TYPE_ENUM, label=field.LABEL_OPTIONAL,
                       type_name='.grpc.health.v1.HealthCheckResponse.ServingStatus')
    return file


_POOL = descriptor_pool.DescriptorPool()
_POOL.AddSerializedFile(_health_file().SerializeToString())
HealthCheckRequest = GetMessageClass(_POOL.FindMessageTypeByName('grpc.health.v1.HealthCheckRequest'))
_RESPONSE_DESCRIPTOR = _POOL.FindMessageTypeByName('grpc.health.v1.HealthCheckResponse')
HealthCheckResponse = GetMessageClass(_RESPONSE_DESCRIPTOR)
_STATUS = _RESPONSE_DESCRIPTOR.enum_types_by_name['ServingStatus']
SERVING = _STATUS.values_by_name['SERVING'].number


def _serialize(request: Message) -> bytes:
    return request.SerializeToString()


def _serving_status(data: bytes) -> int:
    return int(cast(Any, HealthCheckResponse.FromString(data)).status)


def health_check(channel: grpc.Channel) -> grpc.UnaryUnaryMultiCallable[Message, int]:
    """The check on `channel`, answering with the serving status as its number."""
    return channel.unary_unary(CHECK, request_serializer=_serialize, response_deserializer=_serving_status)


def health_check_async(channel: grpc.aio.Channel) -> grpc.aio.UnaryUnaryMultiCallable[Message, int]:
    return channel.unary_unary(CHECK, request_serializer=_serialize, response_deserializer=_serving_status)


def _not_serving(status: int) -> MappedError:
    """A check answered with anything but serving, retried as the service being unavailable."""
    name = _STATUS.values_by_number[status].name if status in _STATUS.values_by_number else str(status)
    return MappedError(CouchbaseException,
                       f'couchbase2 server reported serving status {name}',
                       grpc.StatusCode.OK.value[0],
                       retry_reason=RetryReason.SERVICE_NOT_AVAILABLE)


def wait_until_serving(check: grpc.UnaryUnaryMultiCallable[Message, int], ctx: RequestContext) -> None:
    while True:
        status = call_unary(check, HealthCheckRequest(), ctx, ())
        if status == SERVING:
            return
        time.sleep(ctx.on_failure(_not_serving(status)))


async def wait_until_serving_async(check: grpc.aio.UnaryUnaryMultiCallable[Message, int],
                                   ctx: RequestContext) -> None:
    while True:
        status = await call_unary_async(check, HealthCheckRequest(), ctx, ())
        if status == SERVING:
            return
        await asyncio.sleep(ctx.on_failure(_not_serving(status)))
