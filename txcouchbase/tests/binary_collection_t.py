#  Copyright 2016-2022. Couchbase, Inc.
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

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException)
from couchbase.options import (ClusterOptions,
                               DecrementOptions,
                               DeltaValue,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import CounterResult
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from txcouchbase.cluster import Cluster

from ._test_utils import TestEnvironment, wait_for_deferred


class BinaryCollectionTests:

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        c = Cluster(
            conn_string, opts)
        b = c.bucket(f"{couchbase_config.bucket_name}")
        wait_for_deferred(b.on_connect())
        coll = b.default_collection()
        cb_env = TestEnvironment(c, b, coll, couchbase_config)
        # wait_for_deferred(cb_env.load_binary_data())
        cb_env.load_binary_data()
        yield cb_env
        wait_for_deferred(cb_env.purge_binary_data())
        wait_for_deferred(c.close())

    def test_append_string(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")
        result = wait_for_deferred(cb.binary().append(key, "foo"))
        assert result.cas is not None
        # make sure it really worked
        result = wait_for_deferred(
            cb.get(key, transcoder=RawStringTranscoder()))
        assert result.content_as[str] == "foo"

    def test_append_string_not_empty(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")

        tc = RawStringTranscoder()
        wait_for_deferred(cb.upsert(key, "XXXX", transcoder=tc))
        result = wait_for_deferred(cb.binary().append(key, "foo"))
        assert result.cas is not None
        result = wait_for_deferred(
            cb.get(key, transcoder=RawStringTranscoder()))
        assert result.content_as[str] == "XXXXfoo"

    def test_append_string_nokey(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")
        wait_for_deferred(cb.remove(key))
        try:
            cb_env.try_n_times(10, 1, cb.get, key)
        except CouchbaseException:
            pass

        # TODO:  NotStoredException?
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.binary().append(key, "foo"))

        # reset
        cb_env.load_binary_data()

    def test_append_bytes(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("BYTES")
        result = wait_for_deferred(cb.binary().append(key, b"XXX"))
        assert result.cas is not None
        # make sure it really worked
        result = wait_for_deferred(
            cb.get(key, transcoder=RawBinaryTranscoder()))
        assert result.content_as[bytes] == b"XXX"

    def test_append_bytes_not_empty(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("BYTES")

        tc = RawBinaryTranscoder()
        wait_for_deferred(cb.upsert(key, b"XXXX", transcoder=tc))
        result = wait_for_deferred(cb.binary().append(key, "foo"))
        assert result.cas is not None
        result = wait_for_deferred(cb.get(key, transcoder=tc))
        assert result.content_as[bytes] == b"XXXXfoo"

    def test_prepend_string(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")
        result = wait_for_deferred(cb.binary().prepend(key, "foo"))
        assert result.cas is not None
        # make sure it really worked
        result = wait_for_deferred(
            cb.get(key, transcoder=RawStringTranscoder()))
        assert result.content_as[str] == "foo"

    def test_prepend_string_not_empty(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")

        tc = RawStringTranscoder()
        wait_for_deferred(cb.upsert(key, "XXXX", transcoder=tc))
        result = wait_for_deferred(cb.binary().prepend(key, "foo"))
        assert result.cas is not None
        result = wait_for_deferred(cb.get(key, transcoder=tc))
        assert result.content_as[str] == "fooXXXX"

    def test_prepend_string_nokey(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("UTF8")
        wait_for_deferred(cb.remove(key))
        try:
            cb_env.try_n_times(10, 1, cb.get, key)
        except CouchbaseException:
            pass

        # TODO:  NotStoredException?
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.binary().prepend(key, "foo"))

        # reset
        cb_env.load_binary_data()

    def test_prepend_bytes(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("BYTES")
        result = wait_for_deferred(cb.binary().prepend(key, b"XXX"))
        assert result.cas is not None
        # make sure it really worked
        result = wait_for_deferred(
            cb.get(key, transcoder=RawBinaryTranscoder()))
        assert result.content_as[bytes] == b"XXX"

    def test_prepend_bytes_not_empty(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("BYTES")

        tc = RawBinaryTranscoder()
        wait_for_deferred(cb.upsert(key, b"XXXX", transcoder=tc))
        result = wait_for_deferred(cb.binary().prepend(key, "foo"))
        assert result.cas is not None
        result = wait_for_deferred(cb.get(key, transcoder=tc))
        assert result.content_as[bytes] == b"fooXXXX"

    def test_counter_increment_initial_value(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        result = wait_for_deferred(cb.binary().increment(
            key, IncrementOptions(initial=SignedInt64(100))))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        result = wait_for_deferred(cb.get(key))

        assert result.content_as[int] == 100

        # reset
        cb_env.load_binary_data()

    def test_counter_decrement_initial_value(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        result = wait_for_deferred(cb.binary().decrement(
            key, DecrementOptions(initial=SignedInt64(100))))
        assert isinstance(result, CounterResult)
        assert result.cas is not None
        result = wait_for_deferred(cb.get(key))

        assert result.content_as[int] == 100

        # reset
        cb_env.load_binary_data()

    def test_counter_increment(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        wait_for_deferred(cb.upsert(key, 100))
        wait_for_deferred(cb.binary().increment(key))
        result = wait_for_deferred(cb.get(key))
        assert 101 == result.content_as[int]

    def test_counter_decrement(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        wait_for_deferred(cb.upsert(key, 100))
        wait_for_deferred(cb.binary().decrement(key))
        result = wait_for_deferred(cb.get(key))
        assert 99 == result.content_as[int]

    def test_counter_increment_non_default(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        wait_for_deferred(cb.upsert(key, 100))
        wait_for_deferred(cb.binary().increment(
            key, IncrementOptions(delta=DeltaValue(3))))
        result = wait_for_deferred(cb.get(key))
        assert 103 == result.content_as[int]

    def test_counter_decrement_non_default(self, cb_env):
        cb = cb_env.collection
        key = cb_env.get_binary_key("COUNTER")

        wait_for_deferred(cb.upsert(key, 100))
        wait_for_deferred(cb.binary().decrement(
            key, DecrementOptions(delta=DeltaValue(3))))
        result = wait_for_deferred(cb.get(key))
        assert 97 == result.content_as[int]

    def test_unsigned_int(self):
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(-1)
        with pytest.raises(InvalidArgumentException):
            x = DeltaValue(0x7FFFFFFFFFFFFFFF + 1)

        x = DeltaValue(5)
        assert 5 == x.value

    def test_signed_int_64(self):
        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 2)

        with pytest.raises(InvalidArgumentException):
            x = SignedInt64(0x7FFFFFFFFFFFFFFF + 1)

        x = SignedInt64(0x7FFFFFFFFFFFFFFF)
        assert 0x7FFFFFFFFFFFFFFF == x.value
        x = SignedInt64(-0x7FFFFFFFFFFFFFFF - 1)
        assert -0x7FFFFFFFFFFFFFFF - 1 == x.value
