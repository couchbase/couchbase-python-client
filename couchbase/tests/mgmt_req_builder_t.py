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

import asyncio
import time
import warnings
from collections.abc import Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from acouchbase.protocol.classic._management._view_index_mgmt_impl import AsyncViewIndexMgmtImpl
from couchbase.common._observability.handler import ObservableRequestHandler
from couchbase.common._observability.no_op import NoOpMeter, NoOpTracer
from couchbase.common._observability.observability_types import (ObservabilityInstruments,
                                                                 TracerProtocol,
                                                                 WrappedTracer)
from couchbase.common._operations import Op
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.buckets import (BucketSettings,
                                                 ConflictResolutionType,
                                                 CreateBucketSettings)
from couchbase.common.management.collections import (CollectionSpec,
                                                     CreateCollectionSettings,
                                                     UpdateCollectionSettings)
from couchbase.common.management.options import (ChangePasswordOptions,
                                                 CreateCollectionOptions,
                                                 DropBucketOptions,
                                                 DropCollectionOptions,
                                                 DropUserOptions,
                                                 GetAllUsersOptions,
                                                 GetUserOptions,
                                                 PublishDesignDocumentOptions,
                                                 UpsertUserOptions)
from couchbase.common.management.users import (Group,
                                               Role,
                                               User)
from couchbase.common.management.views import DesignDocumentNamespace
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.warnings import CouchbaseDeprecationWarning, CouchbaseOptionWarning
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.classic._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.protocol.classic._management._user_mgmt_req_builder import UserMgmtRequestBuilder
from couchbase.protocol.classic._management._view_index_mgmt_impl import ViewIndexMgmtImpl
from couchbase.protocol.classic._management._view_index_mgmt_req_builder import ViewIndexMgmtRequestBuilder
from tests.observability_test_impls import RecordingTracer
from tests.test_features import TestProtocol

# These build classic requests and read the dict classic hands its binding.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

BUCKET = 'default'
SCOPE = 'test-scope'
COLLECTION = 'test-collection'
USER = 'test-user'
GROUP = 'test-group'
ADMIN = {'name': 'admin', 'bucket': None, 'scope': None, 'collection': None}
TWO_SECONDS = timedelta(seconds=2)
# On Windows, time.monotonic() can advance in 15.6ms ticks, so what is left of a timeout can read up
# to one tick long.
CLOCK_TICK_MS = round(time.get_clock_info('monotonic').resolution * 1000)
DDOC = 'test-ddoc'


def _obs_handler(op: Op) -> ObservableRequestHandler:
    # See the twin helper in cluster_req_builder_t.py for why the tracer is cast.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(op, instruments)


_BUCKET = BucketMgmtRequestBuilder()
_COLL = CollectionMgmtRequestBuilder()
_USER = UserMgmtRequestBuilder()

_CollectionCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_COLLECTION_CASES: list[_CollectionCase] = [
    ('create, no settings',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, settings None',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, None, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, empty settings',
     lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(),
                                                     obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create, both settings',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(max_expiry=timedelta(seconds=10), history=False),
         obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'max_expiry': 10, 'history': False}),
    ('create, zero max_expiry is sent',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(max_expiry=timedelta(0)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'max_expiry': 0}),
    ('create, keywords and a timeout',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, scope_name=SCOPE, collection_name=COLLECTION, settings=CreateCollectionSettings(history=True),
         timeout=timedelta(seconds=2), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'history': True, 'timeout': 2000}),
    ('create, options and a timeout',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, CreateCollectionSettings(history=True),
         CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'history': True, 'timeout': 2000}),
    ('create, options by keyword',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, SCOPE, COLLECTION, options=CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('create, CollectionSpec and options by keyword',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, collection=CollectionSpec(COLLECTION, SCOPE),
         options=CreateCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('create, CollectionSpec',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, timedelta(seconds=5), history=True), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'max_expiry': 5,
      'history': True}),
    ('create, CollectionSpec carrying only history',
     lambda h: _COLL.build_create_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, history=False), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'history': False}),
    ('update, history only',
     lambda h: _COLL.build_update_collection_request(
         BUCKET, SCOPE, COLLECTION, UpdateCollectionSettings(history=True), h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'history': True}),
    ('update, both settings',
     lambda h: _COLL.build_update_collection_request(
         BUCKET, SCOPE, COLLECTION, UpdateCollectionSettings(max_expiry=timedelta(minutes=1), history=False), h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION,
      'max_expiry': 60, 'history': False}),
    ('drop',
     lambda h: _COLL.build_drop_collection_request(BUCKET, SCOPE, COLLECTION, obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('drop, options by keyword',
     lambda h: _COLL.build_drop_collection_request(
         BUCKET, SCOPE, COLLECTION, options=DropCollectionOptions(timeout=timedelta(seconds=2)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION, 'timeout': 2000}),
    ('drop, CollectionSpec',
     lambda h: _COLL.build_drop_collection_request(
         BUCKET, CollectionSpec(COLLECTION, SCOPE, timedelta(seconds=5)), obs_handler=h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE, 'collection_name': COLLECTION}),
    ('create scope',
     lambda h: _COLL.build_create_scope_request(BUCKET, SCOPE, h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE}),
    ('drop scope',
     lambda h: _COLL.build_drop_scope_request(BUCKET, SCOPE, h),
     {'bucket_name': BUCKET, 'scope_name': SCOPE}),
    ('get all scopes',
     lambda h: _COLL.build_get_all_scopes_request(BUCKET, h),
     {'bucket_name': BUCKET}),
]


_BucketCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_BUCKET_CASES: list[_BucketCase] = [
    ('describe',
     lambda h: _BUCKET.build_bucket_describe_request(BUCKET, h),
     {'name': BUCKET}),
    ('create, minimal settings',
     lambda h: _BUCKET.build_create_bucket_request(CreateBucketSettings(name=BUCKET), h),
     {'bucket': {'name': BUCKET}}),
    ('create, conflict resolution and a timeout',
     lambda h: _BUCKET.build_create_bucket_request(
         CreateBucketSettings(name=BUCKET, conflict_resolution_type=ConflictResolutionType.CUSTOM,
                              num_replicas=0), h, timeout=TWO_SECONDS),
     {'bucket': {'name': BUCKET, 'num_replicas': 0, 'conflict_resolution_type': 'custom'}, 'timeout': 2000}),
    ('drop',
     lambda h: _BUCKET.build_drop_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('drop, options',
     lambda h: _BUCKET.build_drop_bucket_request(BUCKET, h, DropBucketOptions(timeout=TWO_SECONDS)),
     {'name': BUCKET, 'timeout': 2000}),
    ('flush',
     lambda h: _BUCKET.build_flush_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('get all',
     lambda h: _BUCKET.build_get_all_buckets_request(h),
     {}),
    ('get all, timeout',
     lambda h: _BUCKET.build_get_all_buckets_request(h, timeout=TWO_SECONDS),
     {'timeout': 2000}),
    ('get',
     lambda h: _BUCKET.build_get_bucket_request(BUCKET, h),
     {'name': BUCKET}),
    ('update, flush disabled',
     lambda h: _BUCKET.build_update_bucket_request(BucketSettings(name=BUCKET, flush_enabled=False), h),
     {'bucket': {'name': BUCKET, 'flush_enabled': False}}),
    ('update leaves conflict resolution out',
     lambda h: _BUCKET.build_update_bucket_request(
         CreateBucketSettings(name=BUCKET, conflict_resolution_type=ConflictResolutionType.CUSTOM), h),
     {'bucket': {'name': BUCKET}}),
]

_UserCase = tuple[str, Callable[[ObservableRequestHandler], Any], dict[str, Any]]

_USER_CASES: list[_UserCase] = [
    ('change password',
     lambda h: _USER.build_change_password_request('new-password', h),
     {'newPassword': 'new-password'}),
    ('change password, options',
     lambda h: _USER.build_change_password_request('new-password', h, ChangePasswordOptions(timeout=TWO_SECONDS)),
     {'newPassword': 'new-password', 'timeout': 2000}),
    ('drop group',
     lambda h: _USER.build_drop_group_request(GROUP, h),
     {'name': GROUP}),
    ('drop user, local by default',
     lambda h: _USER.build_drop_user_request(USER, h),
     {'username': USER, 'domain': 'local'}),
    ('drop user, external domain',
     lambda h: _USER.build_drop_user_request(USER, h, DropUserOptions(domain_name='external')),
     {'username': USER, 'domain': 'external'}),
    ('get all groups',
     lambda h: _USER.build_get_all_groups_request(h),
     {}),
    ('get all users',
     lambda h: _USER.build_get_all_users_request(h),
     {'domain': 'local'}),
    ('get all users, options',
     lambda h: _USER.build_get_all_users_request(h, GetAllUsersOptions(domain_name='external',
                                                                       timeout=TWO_SECONDS)),
     {'domain': 'external', 'timeout': 2000}),
    ('get group',
     lambda h: _USER.build_get_group_request(GROUP, h),
     {'name': GROUP}),
    ('get roles',
     lambda h: _USER.build_get_roles_request(h, timeout=TWO_SECONDS),
     {'timeout': 2000}),
    ('get user',
     lambda h: _USER.build_get_user_request(USER, h),
     {'username': USER, 'domain': 'local'}),
    ('get user, options',
     lambda h: _USER.build_get_user_request(USER, h, GetUserOptions(domain_name='external')),
     {'username': USER, 'domain': 'external'}),
    ('upsert group',
     lambda h: _USER.build_upsert_group_request(Group(GROUP, description='d', roles=Role('admin')), h),
     {'group': {'name': GROUP, 'description': 'd', 'ldap_group_reference': None, 'roles': [ADMIN]}}),
    ('upsert user, local',
     lambda h: _USER.build_upsert_user_request(User(USER, password='pw', roles=[Role('admin')]), h),
     {'user': {'username': USER, 'display_name': None, 'password': 'pw', 'roles': [ADMIN]}, 'domain': 'local'}),
    ('upsert user, external with groups only',
     lambda h: _USER.build_upsert_user_request(User(USER, groups='g'), h,
                                               UpsertUserOptions(domain_name='external', timeout=TWO_SECONDS)),
     {'user': {'username': USER, 'display_name': None, 'password': None, 'groups': {'g'}},
      'domain': 'external', 'timeout': 2000}),
]


# A keyword no options class carries, under each mode that does not refuse it.
_LENIENT_MODES = [None, OptionValidationMode.Ignore, OptionValidationMode.Warn]


def _quiet(build: Callable[[ObservableRequestHandler], Any], op: Op) -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', CouchbaseDeprecationWarning)
        warnings.simplefilter('ignore', CouchbaseOptionWarning)
        return build(_obs_handler(op))


# No cb_env in any class below: each test builds a request and compares what req_to_dict hands
# the binding, including which keys are absent, since the binding treats a missing key
# differently from a falsy one.


class CollectionMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _COLLECTION_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.CollectionCreate).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = CollectionMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_drop_scope_request(BUCKET, SCOPE, h, not_an_option=1), Op.ScopeDrop)
        assert req.req_to_dict() == {'bucket_name': BUCKET, 'scope_name': SCOPE}

    def test_unknown_keyword_warns(self) -> None:
        builder = CollectionMgmtRequestBuilder(OptionValidationMode.Warn)
        with pytest.warns(CouchbaseOptionWarning, match='not_an_option'):
            builder.build_drop_scope_request(BUCKET, SCOPE, _obs_handler(Op.ScopeDrop), not_an_option=1)

    def test_a_second_options_object_raises(self) -> None:
        opts = CreateCollectionOptions()
        with pytest.raises(TypeError, match='takes one options object, 2 given'):
            _quiet(lambda h: _COLL.build_create_collection_request(BUCKET, SCOPE, COLLECTION, None, opts, opts,
                                                                   obs_handler=h),
                   Op.CollectionCreate)
        with pytest.raises(TypeError, match='takes one options object, 2 given'):
            _quiet(lambda h: _COLL.build_drop_collection_request(BUCKET, CollectionSpec(COLLECTION, SCOPE),
                                                                 DropCollectionOptions(), DropCollectionOptions(),
                                                                 obs_handler=h),
                   Op.CollectionDrop)

    def test_options_positionally_and_by_keyword_raises(self) -> None:
        opts = DropCollectionOptions()
        with pytest.raises(TypeError, match="multiple values for argument 'options'"):
            _quiet(lambda h: _COLL.build_drop_collection_request(BUCKET, SCOPE, COLLECTION, opts, options=opts,
                                                                 obs_handler=h),
                   Op.CollectionDrop)

    @pytest.mark.parametrize('validate, value, message', [
        (_COLL._validate_bucket_name, 7, 'The bucket_name must be str.'),
        (_COLL._validate_scope_name, 7, 'The scope_name must be str.'),
        (_COLL._validate_collection_name, 7, 'The collection_name must be str.'),
        (_COLL._validate_scope_name, None, 'The scope_name cannot be empty.'),
        (_COLL._validate_scope_name, ' ', 'The scope_name cannot be empty.'),
    ])
    def test_name_validator_message(self, validate: Callable[[Any], Any], value: Any, message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            validate(value)
        assert exc_info.value.message == message


class _FakeViewAdapter:
    """Answers every request with an empty design document, `delay` seconds late, and records the requests."""

    option_validation = None

    def __init__(self, delay: float = 0) -> None:
        self.delay = delay
        self.requests: list[Any] = []

    def answer(self, req: Any) -> Any:
        self.requests.append(req)
        time.sleep(self.delay)
        return SimpleNamespace(raw_result={'document': {'name': DDOC, 'views': {}}})

    def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.answer(req)


class _AsyncFakeViewAdapter:

    def __init__(self, delay: float = 0) -> None:
        self.fake = _FakeViewAdapter(delay)
        self.option_validation = None

    async def execute_mgmt_request(self, req: Any, obs_handler: Any = None) -> Any:
        return self.fake.answer(req)


class ViewIndexMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = ViewIndexMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_design_document_request(BUCKET, DDOC,
                                                                         DesignDocumentNamespace.PRODUCTION, h,
                                                                         not_an_option=1),
                     Op.ViewIndexGet)
        assert req.req_to_dict() == {'bucket_name': BUCKET, 'document_name': DDOC, 'ns': 'production'}

    def test_unknown_keyword_warns(self) -> None:
        builder = ViewIndexMgmtRequestBuilder(OptionValidationMode.Warn)
        with pytest.warns(CouchbaseOptionWarning, match='not_an_option'):
            builder.build_get_design_document_request(BUCKET, DDOC, DesignDocumentNamespace.PRODUCTION,
                                                      _obs_handler(Op.ViewIndexGet), not_an_option=1)

    @pytest.mark.parametrize('api', ['sync', 'async'])
    def test_publish_spans_and_timeout(self, api: str) -> None:
        # Publish is a get and an upsert under one publish span: one root with the two
        # sub-requests' spans as its ended children.  The two share the publish timeout, so the
        # upsert gets what the get left.
        tracer = RecordingTracer()
        instruments = ObservabilityInstruments(WrappedTracer(cast(TracerProtocol, tracer), False), NoOpMeter())
        opts = PublishDesignDocumentOptions(timeout=TWO_SECONDS)
        if api == 'sync':
            fake = _FakeViewAdapter(delay=0.2)
            ViewIndexMgmtImpl(cast(Any, fake), BUCKET, instruments).publish_design_document(DDOC, opts)
        else:
            adapter = _AsyncFakeViewAdapter(delay=0.2)
            fake = adapter.fake
            asyncio.run(AsyncViewIndexMgmtImpl(cast(Any, adapter), BUCKET, instruments)
                        .publish_design_document(DDOC, opts))

        assert [span.name for span in tracer.spans] == [Op.ViewIndexPublish.span_name]
        children = tracer.spans[0].children
        assert [span.name for span in children] == [Op.ViewIndexGet.span_name, Op.ViewIndexUpsert.span_name]
        assert all(span.end_time is not None for span in children)
        get_req, up_req = fake.requests
        assert get_req.timeout == 2000
        assert 0 < up_req.timeout <= 1800 + CLOCK_TICK_MS


class BucketMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _BUCKET_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.BucketCreate).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = BucketMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_bucket_request(BUCKET, h, not_an_option=1), Op.BucketGet)
        assert req.req_to_dict() == {'name': BUCKET}

    @pytest.mark.parametrize('value, message', [
        (7, 'The bucket_name must be str.'),
        (None, 'The bucket_name cannot be empty.'),
        (' ', 'The bucket_name cannot be empty.'),
    ])
    def test_name_validator_message(self, value: Any, message: str) -> None:
        with pytest.raises(InvalidArgumentException) as exc_info:
            _BUCKET._validate_bucket_name(value)
        assert exc_info.value.message == message

    @pytest.mark.parametrize('value, message', [
        (7, 'The bucket_name must be str.'),
        (' ', 'The bucket_name cannot be empty.'),
    ])
    def test_settings_name_is_validated(self, value: Any, message: str) -> None:
        # BucketSettings is a dict, so a name set after construction reaches the builder unchecked.
        create_settings = CreateBucketSettings(name=BUCKET)
        create_settings['name'] = value
        settings = BucketSettings(name=BUCKET)
        settings['name'] = value
        builds = [(lambda h: _BUCKET.build_create_bucket_request(create_settings, h), Op.BucketCreate),
                  (lambda h: _BUCKET.build_update_bucket_request(settings, h), Op.BucketUpdate)]
        for build, op in builds:
            with pytest.raises(InvalidArgumentException) as exc_info:
                _quiet(build, op)
            assert exc_info.value.message == message


class UserMgmtRequestBuilderUnitTests:

    @pytest.mark.parametrize('build, expected',
                             [pytest.param(b, e, id=i) for i, b, e in _USER_CASES])
    def test_request_dict(self, build: Callable[[ObservableRequestHandler], Any], expected: dict[str, Any]) -> None:
        assert _quiet(build, Op.UserGet).req_to_dict() == expected

    @pytest.mark.parametrize('mode', _LENIENT_MODES)
    def test_unknown_keyword_is_not_sent(self, mode: OptionValidationMode | None) -> None:
        builder = UserMgmtRequestBuilder(mode)
        req = _quiet(lambda h: builder.build_get_user_request(USER, h, not_an_option=1), Op.UserGet)
        assert req.req_to_dict() == {'username': USER, 'domain': 'local'}
