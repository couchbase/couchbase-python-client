#  Copyright 2016-2024. Couchbase, Inc.
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

import asyncio
from functools import wraps

import pytest

from acouchbase.logic.wrappers import call_async_fn
from acouchbase.transactions.transactions import AsyncWrapper as TxnAsyncWrapper
from couchbase.exceptions import (CouchbaseException,
                                  ErrorMapper,
                                  InternalSDKException)
from couchbase.exceptions import exception as BaseCouchbaseException


class AsyncTestWrapper:

    @classmethod
    def inject_callbacks(cls):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    self.loop.call_soon_threadsafe(ft.set_result, res)

                def on_err(err):
                    self.loop.call_soon_threadsafe(ft.set_exception, err)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err
                call_async_fn(ft, self, fn, *args, **kwargs)
                return ft

            return wrapped_fn

        return decorator


class AsyncTxnTestWrapper:

    @classmethod
    def inject_callbacks(cls):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ft = self.loop.create_future()

                def on_ok(res):
                    self.loop.call_soon_threadsafe(ft.set_result, res)

                def on_err(err):
                    self.loop.call_soon_threadsafe(ft.set_exception, err)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err
                TxnAsyncWrapper.call_async_fn(ft, self, fn, *args, **kwargs)
                return ft

            return wrapped_fn

        return decorator


class AsyncTester:

    def __init__(self, loop):
        self._loop = loop

    @property
    def loop(self):
        return self._loop

    @AsyncTestWrapper.inject_callbacks()
    def use_callback(self, result, **kwargs):
        callback = kwargs.pop('callback')
        callback(result)

    @AsyncTestWrapper.inject_callbacks()
    def use_errback(self, error, **kwargs):
        errback = kwargs.pop('errback')
        errback(error)

    @AsyncTestWrapper.inject_callbacks()
    def fn_failure(self, error, **kwargs):
        cause = kwargs.pop('cause', None)
        if cause is not None:
            raise error from cause
        raise error


class AsyncTxnTester:

    def __init__(self, loop):
        self._loop = loop

    @property
    def loop(self):
        return self._loop

    @AsyncTxnTestWrapper.inject_callbacks()
    def use_callback(self, result, **kwargs):
        callback = kwargs.pop('callback')
        callback(result)

    @AsyncTxnTestWrapper.inject_callbacks()
    def use_errback(self, error, **kwargs):
        errback = kwargs.pop('errback')
        errback(error)

    @AsyncTxnTestWrapper.inject_callbacks()
    def fn_failure(self, error, **kwargs):
        cause = kwargs.pop('cause', None)
        if cause is not None:
            raise error from cause
        raise error


class AsyncUtilityTestSuite:
    TEST_MANIFEST = [
        'test_call_async_fn_callback',
        'test_call_async_fn_errback',
        'test_call_async_fn_wrapped_fn_failure',
        'test_txn_call_async_fn_wrapped_fn_failure',
    ]

    @pytest.mark.parametrize('tester_class', [AsyncTester, AsyncTxnTester])
    @pytest.mark.asyncio
    async def test_call_async_fn_callback(self, tester_class):
        expected = 'Success!'
        tester = tester_class(asyncio.get_event_loop())
        res = await tester.use_callback(expected)
        assert isinstance(res, str)
        assert res == expected

    @pytest.mark.parametrize('err, tester_class', [(BaseCouchbaseException, AsyncTester),
                                                   (CouchbaseException, AsyncTester),
                                                   (SystemError, AsyncTester),
                                                   (BaseException, AsyncTester),
                                                   (KeyboardInterrupt, AsyncTester),
                                                   (SystemExit, AsyncTester),
                                                   (asyncio.CancelledError, AsyncTester),
                                                   (BaseCouchbaseException, AsyncTxnTester),
                                                   (CouchbaseException, AsyncTxnTester),
                                                   (SystemError, AsyncTxnTester),
                                                   (BaseException, AsyncTxnTester),
                                                   (KeyboardInterrupt, AsyncTxnTester),
                                                   (SystemExit, AsyncTxnTester),
                                                   (asyncio.CancelledError, AsyncTxnTester)])
    @pytest.mark.asyncio
    async def test_call_async_fn_errback(self, err, tester_class):
        tester = tester_class(asyncio.get_event_loop())

        expected_error = err()
        # need to convert the BaseCouchbaseException to a Python Exception
        if err.__module__ == 'pycbc_core':
            expected_error = ErrorMapper.build_exception(expected_error)
        with pytest.raises(type(expected_error)):
            await tester.use_errback(expected_error)

    @pytest.mark.parametrize('err', [BaseCouchbaseException,
                                     CouchbaseException,
                                     SystemError,
                                     BaseException,
                                     KeyboardInterrupt,
                                     SystemExit,
                                     asyncio.CancelledError])
    @pytest.mark.asyncio
    async def test_call_async_fn_wrapped_fn_failure(self, err):
        tester = AsyncTester(asyncio.get_event_loop())

        raise_with_cause = False
        # need to convert the BaseCouchbaseException to a Python Exception
        if err.__module__ == 'pycbc_core':
            expected_error = ErrorMapper.build_exception(err())
        else:
            expected_error = err()

        class_name = expected_error.__class__.__name__
        if class_name == 'SystemError':
            raise_with_cause = True

        if raise_with_cause is True:
            # InternalSDKException b/c the SDK translates unknown to InternalSDKException
            with pytest.raises(InternalSDKException):
                await tester.fn_failure(expected_error)
            cause = Exception('Fail!')
            with pytest.raises(type(cause)):
                await tester.fn_failure(expected_error, cause=cause)
        else:
            with pytest.raises(type(expected_error)):
                await tester.fn_failure(expected_error)

    @pytest.mark.parametrize('err', [BaseCouchbaseException,
                                     CouchbaseException,
                                     SystemError,
                                     BaseException,
                                     KeyboardInterrupt,
                                     SystemExit,
                                     asyncio.CancelledError])
    @pytest.mark.asyncio
    async def test_txn_call_async_fn_wrapped_fn_failure(self, err):
        tester = AsyncTxnTester(asyncio.get_event_loop())

        raise_with_cause = False
        # need to convert the BaseCouchbaseException to a Python Exception
        if err.__module__ == 'pycbc_core':
            expected_error = ErrorMapper.build_exception(err())
        else:
            expected_error = err()

        class_name = expected_error.__class__.__name__
        if class_name == 'SystemError':
            raise_with_cause = True

        if raise_with_cause is True:
            # TypeError b/c cause is None where a cause is expected
            with pytest.raises(TypeError):
                await tester.fn_failure(expected_error)
            cause = Exception('Fail!')
            with pytest.raises(type(cause)):
                await tester.fn_failure(expected_error, cause=cause)
        else:
            with pytest.raises(type(expected_error)):
                await tester.fn_failure(expected_error)


class AsyncUtilityTests(AsyncUtilityTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(AsyncUtilityTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(AsyncUtilityTests) if valid_test_method(meth)]
        test_list = set(AsyncUtilityTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
