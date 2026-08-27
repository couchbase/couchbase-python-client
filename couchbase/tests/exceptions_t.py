#  Copyright 2016-2023. Couchbase, Inc.
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

import sys

import pytest

import couchbase.exceptions as E
from couchbase.subdocument import parse_subdocument_status
from tests.environments import CollectionType


class ExceptionTestSuite:
    TEST_MANIFEST = [
        'test_couchbase_exception_base',
        'test_couchbase_exception_positional_message',
        'test_exceptions_create_only_message',
        'test_unknown_subdocument_status_reports_the_status',
    ]

    @pytest.fixture(scope='class', name='cb_exceptions')
    def get_couchbase_exceptions(self):
        couchbase_exceptions = []
        for ex in dir(E):
            exp = getattr(sys.modules['couchbase.exceptions'], ex)
            try:
                if issubclass(exp, E.CouchbaseException) and exp.__name__ != 'CouchbaseException':
                    couchbase_exceptions.append(exp)
            except TypeError:
                pass

        return couchbase_exceptions

    def test_couchbase_exception_base(self):
        base = E.CouchbaseException(message='This is a test message.')
        assert isinstance(base, Exception)
        assert isinstance(base, E.CouchbaseException)
        assert str(base).startswith('<')
        assert 'message=This is a test message.' in str(base)

    def test_couchbase_exception_positional_message(self):
        # Every CouchbaseException subclass takes `message` as its first positional parameter.
        # The base took `base`, so a bare string landed in `_base` and __repr__ then called
        # .err() on it, raising AttributeError from str() rather than reporting the message.
        base = E.CouchbaseException('This is a test message.')
        assert base.message == 'This is a test message.'
        assert str(base) == '<message=This is a test message.>'

    def test_unknown_subdocument_status_reports_the_status(self):
        # The fallback of parse_subdocument_status is the only shipped caller that constructed
        # the base class positionally, so the one branch whose whole job is to describe an
        # unrecognised status was the one that dropped its description.
        with pytest.raises(E.CouchbaseException) as exc_info:
            parse_subdocument_status(0xFFFF, 'some.path', 'some-key')
        message = str(exc_info.value)
        assert 'Status=65535' in message
        assert 'path=some.path' in message
        assert 'key=some-key' in message

    def test_exceptions_create_only_message(self, cb_exceptions):
        for ex in cb_exceptions:
            new_ex = ex('This is a test message.')
            ex_str = str(new_ex)
            assert ex_str.startswith(f'{ex.__name__}(')
            assert 'message=This is a test message.' in ex_str


class ClassicExceptionTests(ExceptionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicExceptionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicExceptionTests) if valid_test_method(meth)]
        test_list = set(ExceptionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_base_env.setup(request.param)
        yield cb_base_env
        cb_base_env.teardown(request.param)
