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

"""Calling the gateway: each attempt of a request, and the wait between them.

Every decision is the `RequestContext`'s; these loops only call and sleep, so the sync and async
pairs differ in nothing else.  Each `RequestContext` call that raises is made outside the `except`
block, so the exception never chains the gRPC error.

A stream is retried only until it returns its first message, an empty one included.  After that a
retry would replay what the caller has already read, so the stream is latched and a failure ends it.
A stream is lazy: nothing is sent until its first message is asked for, and closing it early
cancels the call.  Its attempt's span ends with the stream.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Generator
from typing import (Any,
                    TypeVar,
                    cast)

import grpc
import grpc.aio

from couchbase.protocol.couchbase2._error_mapping import map_rpc_error
from couchbase.protocol.couchbase2._request_context import RequestContext
from couchbase.protocol.couchbase2._tracing import Metadata

_Req = TypeVar('_Req')
_Resp = TypeVar('_Resp')


def call_unary(call: grpc.UnaryUnaryMultiCallable[_Req, _Resp],
               request: _Req,
               ctx: RequestContext,
               metadata: Metadata) -> _Resp:
    while True:
        ctx.check()
        try:
            with ctx.attempt(call, metadata) as attempt:
                return call(request, timeout=ctx.dispatch(), metadata=attempt.metadata)
        except grpc.RpcError as e:
            error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
        time.sleep(ctx.on_failure(error))


async def call_unary_async(call: grpc.aio.UnaryUnaryMultiCallable[_Req, _Resp],
                           request: _Req,
                           ctx: RequestContext,
                           metadata: Metadata) -> _Resp:
    while True:
        ctx.check()
        try:
            with ctx.attempt(call, metadata) as attempt:
                return await call(request, timeout=ctx.dispatch(), metadata=attempt.metadata)
        except grpc.RpcError as e:
            error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
        await asyncio.sleep(ctx.on_failure(error))


def call_stream(call: grpc.UnaryStreamMultiCallable[_Req, _Resp],
                request: _Req,
                ctx: RequestContext,
                metadata: Metadata) -> Generator[_Resp, None, None]:
    while True:
        ctx.check()
        try:
            with ctx.attempt(call, metadata, stream=True) as attempt:
                stream = call(request, timeout=ctx.dispatch(), metadata=attempt.metadata)
                message = next(stream, None)
        except grpc.RpcError as e:
            error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
        else:
            break
        time.sleep(ctx.on_failure(error))

    try:
        while message is not None:
            yield message
            try:
                message = next(stream, None)
            except grpc.RpcError as e:
                attempt.fail()
                error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
            else:
                continue
            ctx.on_stream_failure(error)
    finally:
        stream.cancel()
        attempt.end()


async def _read(stream: grpc.aio.UnaryStreamCall[Any, _Resp]) -> _Resp | None:
    message = await stream.read()
    return None if message is grpc.aio.EOF else cast(_Resp, message)


async def call_stream_async(call: grpc.aio.UnaryStreamMultiCallable[_Req, _Resp],
                            request: _Req,
                            ctx: RequestContext,
                            metadata: Metadata) -> AsyncGenerator[_Resp, None]:
    while True:
        ctx.check()
        try:
            with ctx.attempt(call, metadata, stream=True) as attempt:
                stream = call(request, timeout=ctx.dispatch(), metadata=attempt.metadata)
                message = await _read(stream)
        except grpc.RpcError as e:
            error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
        else:
            break
        await asyncio.sleep(ctx.on_failure(error))

    try:
        while message is not None:
            yield message
            try:
                message = await _read(stream)
            except grpc.RpcError as e:
                attempt.fail()
                error = map_rpc_error(e, readonly=ctx.readonly, op=ctx.op)
            else:
                continue
            ctx.on_stream_failure(error)
    finally:
        stream.cancel()
        attempt.end()
