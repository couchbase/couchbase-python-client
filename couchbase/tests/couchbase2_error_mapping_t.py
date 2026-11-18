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

from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from typing import Any, NoReturn

import grpc
import grpc.aio
import pytest
from google.protobuf import any_pb2
from google.protobuf.message import Message
from google.rpc import error_details_pb2, status_pb2

from couchbase.common._operations import Op
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  AuthenticationException,
                                  AuthorizationFailureException,
                                  BucketAlreadyExistsException,
                                  BucketNotFlushableException,
                                  BucketNotFoundException,
                                  CasMismatchException,
                                  CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  CouchbaseException,
                                  DeltaInvalidException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentNotJsonException,
                                  DocumentNotLockedException,
                                  DocumentTooDeepException,
                                  DurabilityImpossibleException,
                                  FeatureUnavailableException,
                                  GenericErrorContext,
                                  InternalServerFailureException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  NumberTooBigException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException,
                                  RequestCanceledException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException,
                                  SearchIndexNotFoundException,
                                  ServiceUnavailableException,
                                  UnAmbiguousTimeoutException,
                                  ValueTooLargeException)
from couchbase.protocol.couchbase2._error_mapping import (STATUS_DETAILS_KEY,
                                                          MappedError,
                                                          map_rpc_error)
from couchbase.protocol.couchbase2._retry import RetryReason
from tests.event_loop import run_on_own_loop

Code = grpc.StatusCode
FAIL = '/test.Fail/Fail'


def _packed(detail: Message) -> any_pb2.Any:
    packed = any_pb2.Any()
    packed.Pack(detail)
    return packed


def _status(code: Code, message: str = '', *details: Message | any_pb2.Any) -> bytes:
    return status_pb2.Status(code=code.value[0], message=message,
                             details=[d if isinstance(d, any_pb2.Any) else _packed(d) for d in details]
                             ).SerializeToString()


def _resource(resource_type: str, name: str = 'r') -> error_details_pb2.ResourceInfo:
    return error_details_pb2.ResourceInfo(resource_type=resource_type, resource_name=name)


def _violation(violation_type: str) -> error_details_pb2.PreconditionFailure:
    return error_details_pb2.PreconditionFailure(
        violations=[error_details_pb2.PreconditionFailure.Violation(type=violation_type)])


def _error_info(reason: str) -> error_details_pb2.ErrorInfo:
    return error_details_pb2.ErrorInfo(reason=reason)


class _Failed(grpc.RpcError, grpc.Call):
    """A finished call, as the sync stub raises it."""

    def __init__(self, code: Code, details: str = '', trailer: Any = None) -> None:
        self._code, self._details = code, details
        self._trailing = ((STATUS_DETAILS_KEY, trailer),) if trailer is not None else ()

    def code(self) -> Code:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> Any:
        return self._trailing

    def initial_metadata(self) -> Any:
        return ()

    def is_active(self) -> bool:
        return False

    def time_remaining(self) -> float:
        return 0.0

    def cancel(self) -> bool:
        return False

    def add_callback(self, callback: Any) -> bool:
        return False


def _map(code: Code, *details: Message | any_pb2.Any, message: str = 'm', readonly: bool = False) -> MappedError:
    return map_rpc_error(_Failed(code, 'bare', _status(code, message, *details)), readonly=readonly)


RESOURCE_TYPES = ['document', 'bucket', 'scope', 'collection', 'queryindex', 'searchindex', 'path']


class ExceptionTypeUnitTests:

    @pytest.mark.parametrize('resource_type, expected', zip(RESOURCE_TYPES, [
        DocumentNotFoundException, BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException,
        QueryIndexNotFoundException, SearchIndexNotFoundException, PathNotFoundException]))
    def test_not_found(self, resource_type: str, expected: type[CouchbaseException]) -> None:
        assert _map(Code.NOT_FOUND, _resource(resource_type)).exception_type is expected

    def test_a_search_on_a_missing_index_raises_the_query_class_as_classic_does(self) -> None:
        trailer = _status(Code.NOT_FOUND, 'm', _resource('searchindex'))
        mapped = map_rpc_error(_Failed(Code.NOT_FOUND, 'bare', trailer), readonly=True, op=Op.SearchQuery)
        managed = map_rpc_error(_Failed(Code.NOT_FOUND, 'bare', trailer), readonly=True, op=Op.SearchIndexGet)
        assert (mapped.exception_type, managed.exception_type) == (QueryIndexNotFoundException,
                                                                   SearchIndexNotFoundException)

    @pytest.mark.parametrize('resource_type, expected', zip(RESOURCE_TYPES, [
        DocumentExistsException, BucketAlreadyExistsException, ScopeAlreadyExistsException,
        CollectionAlreadyExistsException, QueryIndexAlreadyExistsException, QueryIndexAlreadyExistsException,
        PathExistsException]))
    def test_already_exists(self, resource_type: str, expected: type[CouchbaseException]) -> None:
        assert _map(Code.ALREADY_EXISTS, _resource(resource_type)).exception_type is expected

    @pytest.mark.parametrize('code', [Code.NOT_FOUND, Code.ALREADY_EXISTS])
    @pytest.mark.parametrize('details', [(), (_resource('eventingfunction'),)], ids=['none', 'unknown'])
    def test_no_known_resource_is_generic(self, code: Code, details: tuple[Message, ...]) -> None:
        assert _map(code, *details).exception_type is CouchbaseException

    @pytest.mark.parametrize('violation, expected', [
        ('LOCKED', DocumentLockedException),
        ('NOT_LOCKED', DocumentNotLockedException),
        ('DOC_TOO_DEEP', DocumentTooDeepException),
        ('DOC_NOT_JSON', DocumentNotJsonException),
        ('PATH_MISMATCH', PathMismatchException),
        ('VALUE_OUT_OF_RANGE', InvalidValueException),
        ('PATH_VALUE_OUT_OF_RANGE', NumberTooBigException),
        ('VALUE_TOO_LARGE', ValueTooLargeException),
        ('DOC_NOT_NUMERIC', DeltaInvalidException),
        ('FLUSH_DISABLED', BucketNotFlushableException),
        ('DURABILITY_IMPOSSIBLE', DurabilityImpossibleException),
        ('POISONED_CAS', CouchbaseException),
    ])
    def test_precondition_failed(self, violation: str, expected: type[CouchbaseException]) -> None:
        mapped = _map(Code.FAILED_PRECONDITION, _violation(violation))
        assert mapped.exception_type is expected
        assert mapped.context['precondition_violation'] == violation

    @pytest.mark.parametrize('details', [(), (error_details_pb2.PreconditionFailure(),)],
                             ids=['no detail', 'no violation'])
    def test_precondition_failed_without_a_violation(self, details: tuple[Message, ...]) -> None:
        mapped = _map(Code.FAILED_PRECONDITION, *details)
        assert mapped.exception_type is CouchbaseException
        assert 'precondition_violation' not in mapped.context

    def test_cas_mismatch(self) -> None:
        mapped = _map(Code.ABORTED, _error_info('CAS_MISMATCH'))
        assert mapped.exception_type is CasMismatchException
        assert mapped.context['reason'] == 'CAS_MISMATCH'

    @pytest.mark.parametrize('details', [(), (_error_info('OTHER'),)], ids=['no detail', 'another reason'])
    def test_any_other_abort_is_generic(self, details: tuple[Message, ...]) -> None:
        assert _map(Code.ABORTED, *details).exception_type is CouchbaseException

    def test_error_info_under_another_code_is_context_only(self) -> None:
        mapped = _map(Code.INTERNAL, _error_info('CAS_MISMATCH'))
        assert mapped.exception_type is InternalServerFailureException
        assert mapped.context['reason'] == 'CAS_MISMATCH'

    def test_a_violation_under_another_code_is_context_only(self) -> None:
        mapped = _map(Code.INTERNAL, _violation('LOCKED'))
        assert mapped.exception_type is InternalServerFailureException
        assert mapped.retry_reason is None
        assert mapped.context['precondition_violation'] == 'LOCKED'

    @pytest.mark.parametrize('details, expected', [
        ((), AuthenticationException),
        ((_resource('user'),), AuthenticationException),
        ((_resource('bucket'),), AuthorizationFailureException),
        ((_resource('collection'),), AuthorizationFailureException),
    ])
    def test_permission_denied(self, details: tuple[Message, ...], expected: type[CouchbaseException]) -> None:
        assert _map(Code.PERMISSION_DENIED, *details).exception_type is expected

    @pytest.mark.parametrize('readonly, expected', [(True, UnAmbiguousTimeoutException),
                                                    (False, AmbiguousTimeoutException)])
    def test_deadline_is_ambiguous_unless_readonly(self, readonly: bool, expected: type[CouchbaseException]) -> None:
        assert map_rpc_error(_Failed(Code.DEADLINE_EXCEEDED), readonly=readonly).exception_type is expected

    @pytest.mark.parametrize('code, expected', [
        (Code.INVALID_ARGUMENT, InvalidArgumentException),
        (Code.OUT_OF_RANGE, InvalidArgumentException),
        (Code.UNIMPLEMENTED, FeatureUnavailableException),
        (Code.UNAUTHENTICATED, AuthenticationException),
        (Code.CANCELLED, RequestCanceledException),
        (Code.UNAVAILABLE, ServiceUnavailableException),
        (Code.RESOURCE_EXHAUSTED, CouchbaseException),
        (Code.INTERNAL, InternalServerFailureException),
        (Code.UNKNOWN, CouchbaseException),
        (Code.DATA_LOSS, CouchbaseException),
    ])
    def test_by_code(self, code: Code, expected: type[CouchbaseException]) -> None:
        assert map_rpc_error(_Failed(code), readonly=False).exception_type is expected


class RetryReasonUnitTests:

    @pytest.mark.parametrize('code, details, reason', [
        (Code.UNAVAILABLE, (), RetryReason.SERVICE_NOT_AVAILABLE),
        (Code.FAILED_PRECONDITION, (_violation('LOCKED'),), RetryReason.KV_LOCKED),
    ])
    def test_retried(self, code: Code, details: tuple[Message, ...], reason: RetryReason) -> None:
        assert _map(code, *details).retry_reason is reason

    @pytest.mark.parametrize('code, details', [
        (Code.FAILED_PRECONDITION, (_violation('NOT_LOCKED'),)),
        (Code.FAILED_PRECONDITION, ()),
        (Code.RESOURCE_EXHAUSTED, ()),
        (Code.ABORTED, (_error_info('CAS_MISMATCH'),)),
        (Code.DEADLINE_EXCEEDED, ()),
        (Code.INTERNAL, ()),
    ])
    def test_not_retried(self, code: Code, details: tuple[Message, ...]) -> None:
        assert _map(code, *details).retry_reason is None


class MessageUnitTests:

    def test_the_status_message_wins(self) -> None:
        assert _map(Code.INTERNAL, message='from the status').message == 'from the status'

    def test_the_bare_message_without_one(self) -> None:
        assert _map(Code.INTERNAL, message='').message == 'bare'
        assert map_rpc_error(_Failed(Code.INTERNAL, 'bare'), readonly=False).message == 'bare'

    def test_the_code_without_either(self) -> None:
        assert map_rpc_error(_Failed(Code.DATA_LOSS), readonly=False).message == 'gRPC status DATA_LOSS'


class FallbackUnitTests:
    # A trailer that cannot be read is the same as no trailer: the bare code decides.

    @pytest.mark.parametrize('trailer', [
        b'\xff\xff not a status',
        'a str, which a -bin key never is',
        _status(Code.INTERNAL, 'about some other failure', _resource('bucket')),
    ], ids=['undecodable', 'not bytes', 'another code'])
    def test_an_unusable_trailer_is_ignored(self, trailer: Any) -> None:
        mapped = map_rpc_error(_Failed(Code.NOT_FOUND, 'bare', trailer), readonly=False)
        assert mapped.exception_type is CouchbaseException
        assert mapped.message == 'bare'
        assert mapped.context == {'grpc_status': 'NOT_FOUND'}

    def test_an_undecodable_detail_is_skipped(self) -> None:
        broken = any_pb2.Any(type_url='type.googleapis.com/google.rpc.ResourceInfo', value=b'\xff\xff')
        mapped = _map(Code.NOT_FOUND, broken)
        assert mapped.exception_type is CouchbaseException
        assert 'resource_type' not in mapped.context

    def test_an_unrelated_detail_is_skipped(self) -> None:
        assert _map(Code.NOT_FOUND, error_details_pb2.Help(), _resource(
            'scope')).exception_type is ScopeNotFoundException

    def test_the_first_block_of_a_type_is_read(self) -> None:
        mapped = _map(Code.NOT_FOUND, _resource('bucket', 'first'), _resource('scope', 'second'))
        assert mapped.exception_type is BucketNotFoundException
        assert mapped.context['resource_name'] == 'first'

    def test_an_error_that_is_not_a_call(self) -> None:
        mapped = map_rpc_error(grpc.RpcError('raised outside a call'), readonly=False)
        assert mapped.exception_type is CouchbaseException
        assert mapped.status_code == Code.UNKNOWN.value[0]
        assert mapped.message == 'raised outside a call'


class ContextUnitTests:

    def test_every_detail(self) -> None:
        mapped = _map(Code.NOT_FOUND,
                      _resource('collection', 'default/_default/missing'),
                      error_details_pb2.DebugInfo(detail='d', stack_entries=['a', 'b']),
                      error_details_pb2.RequestInfo(request_id='r1', serving_data='s'))
        assert mapped.context == {
            'grpc_status': 'NOT_FOUND',
            'resource_name': 'default/_default/missing',
            'resource_type': 'collection',
            'debug_info': {'detail': 'd', 'stack_trace': ['a', 'b']},
            'request_info': {'request_id': 'r1', 'serving_data': 's'},
        }

    def test_empty_debug_and_request_info_are_still_reported(self) -> None:
        mapped = _map(Code.INTERNAL, error_details_pb2.DebugInfo(), error_details_pb2.RequestInfo())
        assert mapped.context == {'grpc_status': 'INTERNAL',
                                  'debug_info': {'detail': '', 'stack_trace': []},
                                  'request_info': {'request_id': '', 'serving_data': ''}}

    def test_no_detail_is_the_status_alone(self) -> None:
        assert _map(Code.INTERNAL).context == {'grpc_status': 'INTERNAL'}


class ExceptionUnitTests:

    def test_built_from_plain_values(self) -> None:
        mapped = _map(Code.ABORTED, _error_info('CAS_MISMATCH'), message='cas changed')
        exc = mapped.exception(retry_attempts=2, retry_reasons={'key_value_locked'}, last_exception='earlier')
        assert type(exc) is CasMismatchException
        assert exc.message == 'cas changed'
        assert exc.error_code is None
        assert isinstance(exc.error_context, GenericErrorContext)
        assert exc.error_context.retry_attempts == 2
        assert exc.error_context.retry_reasons == {'key_value_locked'}
        assert exc.error_context.last_exception == 'earlier'
        assert exc.error_context._get_base()['reason'] == 'CAS_MISMATCH'
        assert exc.error_context._get_base()['grpc_status'] == 'ABORTED'

    def test_rendered_with_the_gateway_message_and_the_status(self) -> None:
        exc = _map(Code.ABORTED, _error_info('OTHER'), message='the gateway gave up').exception()
        assert type(exc) is CouchbaseException
        rendered = str(exc)
        assert 'message=the gateway gave up' in rendered
        assert "'grpc_status': 'ABORTED'" in rendered
        assert "'reason': 'OTHER'" in rendered

    def test_each_build_is_a_new_exception(self) -> None:
        mapped = _map(Code.NOT_FOUND, _resource('document'))
        first, second = mapped.exception(retry_attempts=1), mapped.exception()
        assert first is not second
        assert 'retry_attempts' not in second.error_context._get_base()
        assert 'retry_attempts' not in mapped.context


def _fail(request: bytes, context: grpc.ServicerContext) -> NoReturn:
    context.set_trailing_metadata(((STATUS_DETAILS_KEY, _status(Code.NOT_FOUND, 'no such scope',
                                                                _resource('scope', 'default/missing'))),))
    context.abort(Code.NOT_FOUND, 'bare')


@contextmanager
def _server() -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        'test.Fail', {'Fail': grpc.unary_unary_rpc_method_handler(_fail)}),))
    port = server.add_insecure_port('localhost:0')
    server.start()
    try:
        yield f'localhost:{port}'
    finally:
        server.stop(None)


class RaisedByGrpcUnitTests:
    # The errors each stub type really raises, read through the trailer a server sent.

    def _check(self, error: BaseException) -> None:
        assert isinstance(error, grpc.RpcError)
        mapped = map_rpc_error(error, readonly=True)
        assert mapped.exception_type is ScopeNotFoundException
        assert mapped.message == 'no such scope'
        assert mapped.context['resource_name'] == 'default/missing'

    def test_sync(self) -> None:
        with _server() as target, grpc.insecure_channel(target) as channel:
            with pytest.raises(grpc.RpcError) as ex:
                channel.unary_unary(FAIL, bytes, bytes)(b'', timeout=10)
        self._check(ex.value)

    def test_aio(self) -> None:
        async def call(target: str) -> BaseException:
            async with grpc.aio.insecure_channel(target) as channel:
                try:
                    await channel.unary_unary(FAIL, bytes, bytes)(b'', timeout=10)
                except grpc.aio.AioRpcError as e:
                    return e
            raise AssertionError('the call succeeded')

        with _server() as target:
            self._check(run_on_own_loop(call(target)))


class UnrecognizedUnitTests:

    @pytest.mark.parametrize('code, details, named', [
        (Code.NOT_FOUND, (_resource('vbucket'),), 'NOT_FOUND resource type "vbucket"'),
        (Code.ALREADY_EXISTS, (_resource('vbucket'),), 'ALREADY_EXISTS resource type "vbucket"'),
        (Code.FAILED_PRECONDITION, (_violation('POISONED_CAS'),), 'precondition violation "POISONED_CAS"'),
        (Code.ABORTED, (_error_info('CONFLICT'),), 'ABORTED reason "CONFLICT"'),
        (Code.DATA_LOSS, (), 'gRPC status DATA_LOSS'),
    ], ids=['not-found', 'already-exists', 'precondition', 'aborted', 'status'])
    def test_a_value_with_no_mapping_is_named(self, code: Code, details: tuple[Message, ...], named: str) -> None:
        assert _map(code, *details).unrecognized == named

    @pytest.mark.parametrize('code, details', [
        (Code.NOT_FOUND, (_resource('document'),)),
        (Code.NOT_FOUND, ()),
        (Code.ALREADY_EXISTS, (_resource('searchindex'),)),
        (Code.FAILED_PRECONDITION, (_violation('LOCKED'),)),
        (Code.FAILED_PRECONDITION, ()),
        (Code.ABORTED, (_error_info('CAS_MISMATCH'),)),
        (Code.ABORTED, ()),
        (Code.PERMISSION_DENIED, (_resource('bucket'),)),
        (Code.DEADLINE_EXCEEDED, ()),
        (Code.UNKNOWN, ()),
        (Code.UNAVAILABLE, ()),
    ], ids=['resource', 'no-resource', 'search-index', 'violation', 'no-violation', 'cas-mismatch',
            'no-reason', 'permission', 'deadline', 'unknown', 'by-code'])
    def test_a_mapped_value_is_not(self, code: Code, details: tuple[Message, ...]) -> None:
        assert _map(code, *details).unrecognized is None
