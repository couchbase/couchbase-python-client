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
from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast

import pytest

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
from couchbase.common.management.options import (ChangePasswordOptions,
                                                 DropBucketOptions,
                                                 DropUserOptions,
                                                 GetAllUsersOptions,
                                                 GetUserOptions,
                                                 UpsertUserOptions)
from couchbase.common.management.users import (Group,
                                               Role,
                                               User)
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.warnings import CouchbaseDeprecationWarning, CouchbaseOptionWarning
from couchbase.protocol.classic._management._bucket_mgmt_req_builder import BucketMgmtRequestBuilder
from couchbase.protocol.classic._management._user_mgmt_req_builder import UserMgmtRequestBuilder
from tests.test_features import TestProtocol

# These build classic requests and read the dict classic hands its binding.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

BUCKET = 'default'
USER = 'test-user'
GROUP = 'test-group'
ADMIN = {'name': 'admin', 'bucket': None, 'scope': None, 'collection': None}
TWO_SECONDS = timedelta(seconds=2)


def _obs_handler(op: Op) -> ObservableRequestHandler:
    # See the twin helper in cluster_req_builder_t.py for why the tracer is cast.
    tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
    instruments = ObservabilityInstruments(tracer, NoOpMeter(), is_noop=True)
    return ObservableRequestHandler(op, instruments)


_BUCKET = BucketMgmtRequestBuilder()
_USER = UserMgmtRequestBuilder()


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
