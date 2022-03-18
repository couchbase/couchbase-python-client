import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps

import pytest

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DurabilityImpossibleException,
                                  InternalSDKException,
                                  InvalidArgumentException,
                                  PathNotFoundException,
                                  TemporaryFailException)
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               InsertOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetResult,
                              MutationResult)
from couchbase.tests._test_utils import CollectionType, TestEnvironment
from tests.helpers import load_config


class SkipTestException(Exception):
    """Raised when skipping a test
    """


class KVPairType(Enum):
    Default = 1
    DefaultWithReset = 2
    NewWithReset = 3
    DefaultAndNewWithReset = 4


class TestStatus(Enum):
    Passed = 'PASSED'
    Failed = 'FAILED'
    Skipped = 'SKIPPED'


class TerminalColors(Enum):
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    SUCCESS = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @classmethod
    def output_message(cls, term_color, msg):
        output = []
        if term_color == cls.BLUE:
            output.append(cls.BLUE.value)
        elif term_color == cls.CYAN:
            output.append(cls.CYAN.value)
        elif term_color in [cls.GREEN, cls.SUCCESS]:
            output.append(cls.GREEN.value)
        elif term_color == cls.WARNING:
            output.append(cls.WARNING.value)
        elif term_color == cls.FAIL:
            output.append(cls.FAIL.value)

        output.extend([msg, cls.ENDC.value])

        return ''.join(output)


class TestResult:
    def __init__(self, status, msg=None):
        self._status = status
        self._msg = msg

    @property
    def status(self):
        return self._status

    @property
    def message(self):
        return self._msg

    def __repr__(self):
        return f'TestResult: {self._status}, {self._msg}'


def setup_test(kvp, checks=None, debug_log=False):
    def decorator(fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            key, value, new_key, new_value = None, None, None, None
            if kvp in [KVPairType.Default, KVPairType.DefaultWithReset, KVPairType.DefaultAndNewWithReset]:
                key, value = self.cb_env.get_default_key_value()
            # elif kvp == KVPairType.NewWithReset:
            #     key, value = self.cb_env.get_new_key_value()

            if kvp in [KVPairType.NewWithReset, KVPairType.DefaultAndNewWithReset]:
                print('trying to get new key and value')
                new_key, new_value = self.cb_env.get_new_key_value(debug_log=debug_log)

            kwargs['key'] = key
            kwargs['value'] = value
            kwargs['new_key'] = new_key
            kwargs['new_value'] = new_value
            fn(self, *args, **kwargs)

            if kvp in [KVPairType.DefaultWithReset, KVPairType.DefaultAndNewWithReset]:
                self.cb_env.collection.upsert(key, value)
            if kvp in [KVPairType.NewWithReset, KVPairType.DefaultAndNewWithReset]:
                print(f'removing key: {new_key}')
                self.cb_env.try_n_times_till_exception(10,
                                                       1,
                                                       self.cb_env.collection.remove,
                                                       new_key,
                                                       expected_exceptions=(DocumentNotFoundException,))
                print(f'done w/ wrapped_fn')

        return wrapped_fn

    return decorator


class CollectionTestSuite:
    NO_KEY = "not-a-key"
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    def __init__(self, couchbase_config, collection_type):
        self.collection_type = collection_type
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        c = Cluster(
            conn_string, opts)
        c.cluster_info()
        b = c.bucket(f"{couchbase_config.bucket_name}")
        coll = b.default_collection()
        if collection_type == CollectionType.DEFAULT:
            self.cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True)
        elif collection_type == CollectionType.NAMED:
            self.cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True, manage_collections=True)
            self.cb_env.setup_named_collections()

        self.cb_env.load_data()
        self.collection_type = collection_type

    def do_tests(self):
        self.exists()
        self.does_not_exist()
        self.get()
        self.get_options()
        self.get_fails()
        self.upsert_preserve_expiry_not_used()
        self.get_with_expiry()
        self.expiry_really_expires()
        self.project()
        self.project_bad_path()
        self.project_project_not_list()
        self.project_too_many_projections()

    def shutdown(self):
        self.cb_env.purge_data()
        if self.collection_type == CollectionType.NAMED:
            self.cb_env.teardown_named_collections()
        self.cb_env.cluster.close()

    @setup_test(KVPairType.Default)
    def exists(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        result = cb.exists(key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    @setup_test(KVPairType.Default)
    def does_not_exist(self, **kwargs):
        cb = self.cb_env.collection
        result = cb.exists(self.NO_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    @setup_test(KVPairType.Default)
    def get(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    @setup_test(KVPairType.Default)
    def get_options(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        result = cb.get(key, GetOptions(
            timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def get_fails(self, **kwargs):
        cb = self.cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            cb.get(self.NO_KEY)

    # @pytest.mark.usefixtures("check_xattr_supported")
    @setup_test(KVPairType.NewWithReset)
    def get_with_expiry(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('new_key')
        value = kwargs.get('new_value')
        #print('get_with_expiry - doing upsert()')
        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=10)))

        expiry_path = "$document.exptime"
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry = res.content_as[int](0)
        assert expiry is not None
        assert expiry > 0
        expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
        # when running local, this can be be up to 1050, so just make sure > 0
        assert expires_in > 0

    @setup_test(KVPairType.NewWithReset)
    def expiry_really_expires(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('new_key')
        value = kwargs.get('new_value')
        result = cb.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))
        assert result.cas != 0

        self.cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            cb.get(key)

    @setup_test(KVPairType.Default)
    def project(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        result = cb.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        self.cb_env.try_n_times(10, 3, cas_matches, cb, result.cas)
        result = cb.get(key, GetOptions(project=["faa"]))
        assert {"faa": "ORD"} == result.content_as[dict]
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None

    @setup_test(KVPairType.Default)
    def project_bad_path(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        try:
            cb.get(key, GetOptions(project=["some", "qzx"]))
        except Exception as ex:
            assert isinstance(ex, PathNotFoundException), f'PathNotFoundException not found, exception: {ex}'

    @setup_test(KVPairType.Default)
    def project_not_list(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        # TODO:  better exception
        # with pytest.raises(Exception, match=r"Unable to perform kv operation\."):
        try:
            cb.get(key, GetOptions(project="thiswontwork"))
        except Exception as ex:
            assert isinstance(ex, InternalSDKException), f'InternalSDKException not found, exception: {ex}'

    @setup_test(KVPairType.Default)
    def project_too_many_projections(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        project = []
        for _ in range(17):
            project.append("something")

        try:
            cb.get(key, GetOptions(project=project))
        except Exception as ex:
            assert isinstance(ex, InvalidArgumentException), f'InvalidArgumentException not found, exception: {ex}'

    @setup_test(KVPairType.Default)
    def upsert(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        result = cb.upsert(key, value, UpsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = self.cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @setup_test(KVPairType.DefaultAndNewWithReset)
    def upsert_preserve_expiry_not_used(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        value1 = kwargs.get('new_value')
        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb.upsert(key, value1)
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        self.cb_env.sleep(3.0)
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    # @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @setup_test(KVPairType.DefaultAndNewWithReset)
    def upsert_preserve_expiry(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        value1 = kwargs.get('new_value')

        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb.upsert(key, value1, UpsertOptions(preserve_expiry=True))
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        self.cb_env.sleep(3.0)
        try:
            cb.get(key)
        except Exception as ex:
            assert isinstance(ex, DocumentNotFoundException), f'DocumentNotFoundException not found, exception: {ex}'

    @setup_test(KVPairType.NewWithReset)
    def insert(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('new_key')
        value = kwargs.get('new_value')
        result = cb.insert(key, value, InsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = self.cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @setup_test(KVPairType.Default)
    def insert_document_exists(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        try:
            cb.insert(key, value)
        except Exception as ex:
            assert isinstance(ex, DocumentExistsException), f'DocumentExistsException not found, exception: {ex}'

    @setup_test(KVPairType.DefaultWithReset)
    def replace(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        result = cb.replace(key, value, ReplaceOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = self.cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @setup_test(KVPairType.DefaultAndNewWithReset)
    def replace_with_cas(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value1 = kwargs.get('new_value')
        result = cb.get(key)
        old_cas = result.cas
        result = cb.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        try:
            cb.replace(key, value1, ReplaceOptions(cas=old_cas))
        except Exception as ex:
            assert isinstance(ex, CasMismatchException), f'CasMismatchException not found, exception: {ex}'

    def replace_fail(self):
        cb = self.cb_env.collection

        try:
            cb.replace(self.NO_KEY, {"some": "content"})
        except Exception as ex:
            assert isinstance(ex, DocumentNotFoundException), f'DocumentNotFoundException not found, exception: {ex}'

    @setup_test(KVPairType.DefaultWithReset)
    def remove(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        print(f'key: {key}')
        result = cb.remove(key)
        assert isinstance(result, MutationResult)

        # try:
        #     self.cb_env.try_n_times(3, 1, cb.get, key, expected_exceptions=(
        #         DocumentNotFoundException,))
        # except Exception as ex:
        #     assert isinstance(ex, DocumentNotFoundException), f'DocumentNotFoundException not found, exception: {ex}'

    def remove_fail(self):
        cb = self.cb_env.collection
        try:
            print('remove_fail!')
            cb.remove(self.NO_KEY)
        except Exception as ex:
            assert isinstance(ex, DocumentNotFoundException), f'DocumentNotFoundException not found, exception: {ex}'

    # @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @setup_test(KVPairType.DefaultAndNewWithReset)
    def replace_preserve_expiry_not_used(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        value1 = kwargs.get('new_value')

        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb.replace(key, value1)
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        self.cb_env.sleep(3.0)
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    # @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @setup_test(KVPairType.DefaultAndNewWithReset, debug_log=True)
    def replace_preserve_expiry(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')
        value1 = kwargs.get('new_value')

        print(f'key: {key}, value: {value}, value1: {value1}')

        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)), print_kwargs=True)
        expiry_path = "$document.exptime"
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb.replace(key, value1, ReplaceOptions(preserve_expiry=True))
        res = self.cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        self.cb_env.sleep(3.0)
        try:
            cb.get(key)
        except Exception as ex:
            assert isinstance(ex, DocumentNotFoundException), f'DocumentNotFoundException not found, exception: {ex}'

    # @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @setup_test(KVPairType.DefaultAndNewWithReset, debug_log=True)
    def replace_preserve_expiry_fail(self, **kwargs):
        cb = self.cb_env.collection
        key = kwargs.get('key')
        value = kwargs.get('value')

        opts = ReplaceOptions(
            expiry=timedelta(
                seconds=5),
            preserve_expiry=True)
        try:
            cb.replace(key, value, opts)
        except Exception as ex:
            assert isinstance(ex, InvalidArgumentException), f'InvalidArgumentException not found, exception: {ex}'


def run_tests(config, collection_type):
    test_suite = CollectionTestSuite(config, collection_type)
    tests = {
        'exists': test_suite.exists,
        'does_not_exist': test_suite.does_not_exist,
        'get': test_suite.get,
        'get_options': test_suite.get_options,
        'get_fails': test_suite.get_fails,
        'get_with_expiry': test_suite.get_with_expiry,
        'expiry_really_expires': test_suite.expiry_really_expires,
        'project': test_suite.project,
        'project_bad_path': test_suite.project_bad_path,
        'project_not_list': test_suite.project_not_list,
        'project_too_many_projections': test_suite.project_too_many_projections,
        'upsert': test_suite.upsert,
        'upsert_preserve_expiry_not_used': test_suite.upsert_preserve_expiry_not_used,
        'upsert_preserve_expiry': test_suite.upsert_preserve_expiry,
        'insert': test_suite.insert,
        'insert_document_exists': test_suite.insert_document_exists,
        'replace': test_suite.replace,
        'replace_with_cas': test_suite.replace_with_cas,
        'replace_fail': test_suite.replace_fail,
        'remove': test_suite.remove,
        'remove_fail': test_suite.remove_fail,
        'replace_preserve_expiry_not_used': test_suite.replace_preserve_expiry_not_used,
        'replace_preserve_expiry': test_suite.replace_preserve_expiry,
        'replace_preserve_expiry_fail': test_suite.replace_preserve_expiry_fail,
    }
    test_results = []
    lock = threading.Lock()
    try:
        for test_name, test in tests.items():
            try:
                # if test_name == 'remove':
                #print(f'running test: {test_name}')
                test()
                #print(f'done with {test_name} - {TestStatus.Passed}')
                # if not lock.locked():
                #     lock.acquire()

                test_results.append(TestResult(TestStatus.Passed))
                # lock.release()
                #test_results[test_name] = {'status':TestStatus.Passed.value,'msg':None}
            except SkipTestException:
                test_results.append(TestResult(TestStatus.Skipped))
                #test_results[test_name] = {'status':TestStatus.Skipped.value,'msg':None}
            except AssertionError as err:
                #print(f'done with {test_name} - {TestStatus.Failed}')
                test_results.append(TestResult(TestStatus.Failed, str(err)))
                #test_results[test_name] = {'status':TestStatus.Failed.value,'msg':str(err)}
    except Exception as ex:
        print(f'run_tests() had exception: {ex}')
    finally:
        #print('trying to shutdown')
        test_suite.shutdown()

    # output_results_summary(test_results)
    # if not lock.locked():
    #     lock.acquire()
    # print(test_results)
    # lock.release()


def output_results_summary(test_results):
    print(test_results)
    test_results_output = {
        TestStatus.Passed: [],
        TestStatus.Failed: [],
        TestStatus.Skipped: []
    }
    test_results_count = {
        TestStatus.Passed: 0,
        TestStatus.Failed: 0,
        TestStatus.Skipped: 0
    }
    for k, v in test_results.items():
        print(k)
        print(v)
        if v['status'] == TestStatus.Passed.value:
            msg = TerminalColors.output_message(TerminalColors.SUCCESS, v['status'])
            test_results_output[TestStatus.Passed].append(f'{msg} : {k}')
            test_results_count[TestStatus.Passed] += 1
        elif v['status'] == TestStatus.Failed.value:
            msg = TerminalColors.output_message(TerminalColors.FAIL, v['status'])
            test_results_output[TestStatus.Failed].append(f'{msg} : {k}')
            test_results_count[TestStatus.Failed] += 1
        elif v['status'] == TestStatus.Skipped.value:
            msg = TerminalColors.output_message(TerminalColors.WARNING, v['status'])
            test_results_output[TestStatus.Skipped].append(f'{msg} : {k}')
            test_results_count[TestStatus.Skipped] += 1

    header = ['==============================']
    if test_results_count[TestStatus.Passed] > 0:
        header.append(f'Passed: {test_results_count[TestStatus.Passed]}')
    if test_results_count[TestStatus.Failed] > 0:
        header.append(f'Failed: {test_results_count[TestStatus.Failed]}')
    if test_results_count[TestStatus.Skipped] > 0:
        header.append(f'Skipped: {test_results_count[TestStatus.Skipped]}')

    header.append('==============================')
    output = [' '.join(header)]
    if len(test_results_output[TestStatus.Passed]) > 0:
        output.extend(test_results_output[TestStatus.Passed])
    if len(test_results_output[TestStatus.Failed]) > 0:
        output.extend(test_results_output[TestStatus.Failed])
    if len(test_results_output[TestStatus.Skipped]) > 0:
        output.extend(test_results_output[TestStatus.Skipped])
    output.append('')
    print('\n'.join(output))


if __name__ == "__main__":
    config = load_config()
    run_tests(config, CollectionType.DEFAULT)
    # print(f'\033[92mHello!\033[0m')
