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
from tests.environments import CollectionType


class ExceptionTestSuite:
    TEST_MANIFEST = [
        'test_exceptions_create_only_message',
    ]

    @pytest.fixture(scope='class', name='cb_exceptions')
    def get_couchbase_exceptions(self):
        couchbase_exceptions = []
        skip_list = [
            'CouchbaseException',
            'CryptoException',
            'EncryptionFailureException',
            'DecryptionFailureException',
            'CryptoKeyNotFoundException',
            'InvalidCryptoKeyException',
            'EncrypterNotFoundException',
            'DecrypterNotFoundException',
            'EncrypterAlreadyExistsException',
            'DecrypterAlreadyExistsException',
            'InvalidCipherTextException'
        ]
        for ex in dir(E):
            exp = getattr(sys.modules['couchbase.exceptions'], ex)
            try:
                if issubclass(exp, E.CouchbaseException) and exp.__name__ not in skip_list:
                    couchbase_exceptions.append(exp)
            except TypeError:
                pass

        return couchbase_exceptions

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
