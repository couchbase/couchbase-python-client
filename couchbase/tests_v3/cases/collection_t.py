# -*- coding:utf-8 -*-
#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from couchbase_tests.base import CollectionTestCase
from couchbase.cluster import ClusterOptions, ClassicAuthenticator, PasswordAuthenticator
from couchbase.collection import GetOptions, UpsertOptions, ReplaceOptions, InsertOptions, \
    RemoveOptions
from couchbase.durability import ServerDurability, ClientDurability, Durability, PersistTo, ReplicateTo
from couchbase.exceptions import InvalidArgumentException,  DocumentExistsException, DocumentNotFoundException, \
    TemporaryFailException, PathNotFoundException
import unittest
from datetime import timedelta
from unittest import SkipTest
from couchbase.diagnostics import ServiceType
import logging
from couchbase.management.collections import CollectionSpec
import uuid
from datetime import datetime


class CollectionTests(CollectionTestCase):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some":"content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"

    def setUp(self):
        super(CollectionTests, self).setUp()
        self.cb.upsert(self.KEY, self.CONTENT)
        # make sure it is available
        self.try_n_times(10, 1, self.cb.get, self.KEY)
        # be sure NOKEY isn't in there
        try:
            self.cb.remove(self.NOKEY)
        except DocumentNotFoundException as e:
            pass
        # make sure NOKEY is gone
        self.try_n_times_till_exception(10, 1, self.cb.get, self.NOKEY)

    def test_exists(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        self.assertTrue(self.cb.exists(self.KEY).exists)

    def test_exists_when_it_does_not_exist(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        key = str(uuid.uuid4())
        self.assertRaises(DocumentNotFoundException, self.cb.get, key)
        self.assertFalse(self.cb.exists(key).exists)

    def test_exists_with_recently_removed_key(self):
        if self.is_mock:
            raise SkipTest("mock does not support exists")
        self.cb.remove(self.KEY)
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)
        self.assertFalse(self.cb.exists(self.KEY).exists)

    def test_upsert(self):
        self.cb.upsert(self.NOKEY, {"some":"thing"}, UpsertOptions(timeout=timedelta(seconds=3)))
        result = self.try_n_times(10, 1, self.cb.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some":"thing"}, result.content_as[dict])

    def test_insert(self):
        self.cb.insert(self.NOKEY, {"some": "thing"})
        result = self.try_n_times(10, 1, self.cb.get, self.NOKEY)
        self.assertEqual(self.NOKEY, result.id)
        self.assertDictEqual({"some":"thing"}, result.content_as[dict])

    def test_insert_fail(self):
        self.assertRaises(DocumentExistsException, self.cb.insert, self.KEY, self.CONTENT)

    def test_replace(self):
        result = self.cb.replace(self.KEY, {"some":"other content"})
        self.assertTrue(result.success)

    def test_replace_with_cas(self):
        old_cas = self.cb.get(self.KEY).cas
        result = self.cb.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))
        self.assertTrue(result.success)
        # try same cas again, must fail.  TODO: this seems wrong - lets be sure there
        # isn't perhaps a more sensible exception out there.
        self.assertRaises(DocumentExistsException, self.cb.replace, self.KEY, self.CONTENT, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self):
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.NOKEY)
        self.assertRaises(DocumentNotFoundException, self.cb.replace, self.NOKEY, self.CONTENT)

    def test_remove(self):
        result = self.cb.remove(self.KEY)
        self.assertTrue(result.success)
        self.try_n_times_till_exception(10, 1, self.cb.get, self.KEY)

    def test_remove_fail(self):
        self.assertRaises(DocumentNotFoundException, self.cb.remove, self.NOKEY)

    def test_get(self):
        result = self.cb.get(self.KEY)
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_get_options(self):
        result = self.cb.get(self.KEY, GetOptions(timeout=timedelta(seconds=2), with_expiry=False))
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_get_fails(self):
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.NOKEY)

    def test_expiry_really_expires(self):
        result = self.coll.upsert(self.KEY, self.CONTENT, UpsertOptions(expiry=timedelta(seconds=3)))
        self.assertTrue(result.success)
        self.try_n_times_till_exception(10, 2, self.coll.get, self.KEY)

    def test_get_with_expiry(self):
        if self.is_mock:
            raise SkipTest("mock will not return the expiry in the xaddrs")
        cas = self.coll.upsert(self.KEY, self.CONTENT, UpsertOptions(expiry=timedelta(seconds=1000))).cas

        def cas_matches(c, new_cas):
            r = c.get(self.KEY, GetOptions(with_expiry=True))
            if r.cas == new_cas:
                return r
            raise Exception("nope")
        result = self.try_n_times(10, 3, cas_matches, self.coll, cas)
        self.assertIsNotNone(result.expiry)
        self.assertDictEqual(self.CONTENT, result.content_as[dict])
        expires_in = (result.expiry - datetime.now()).total_seconds()
        self.assertTrue(1000 >= expires_in > 0, msg="Expected expires_in {} to be between 100 and 0")

    def test_project(self):
        content = {"a": "aaa", "b": "bbb", "c": "ccc"}
        cas = self.coll.upsert(self.KEY, content).cas

        def cas_matches(c, new_cas):
            if new_cas != c.get(self.KEY).cas:
                raise Exception("nope")

        self.try_n_times(10, 3, cas_matches, self.coll, cas)
        result = self.coll.get(self.KEY, GetOptions(project=["a"]))
        self.assertEqual({"a": "aaa"}, result.content_as[dict])
        self.assertIsNotNone(result.cas)
        self.assertEqual(result.id, self.KEY)
        self.assertIsNone(result.expiry)

    def test_project_bad_path(self):
        result = self.coll.get(self.KEY, GetOptions(project=["some", "qzx"]))
        self.assertTrue(result.success)
        with self.assertRaisesRegex(PathNotFoundException, 'qzx'):
            result.content_as[dict]

    def test_project_bad_project_string(self):
        with self.assertRaises(InvalidArgumentException):
            self.coll.get(self.KEY, GetOptions(project="something"))

    def test_project_bad_project_too_long(self):
        project = []
        for _ in range(17):
            project.append("something")

        with self.assertRaisesRegex(InvalidArgumentException, "16 operations or less"):
            self.coll.get(self.KEY, GetOptions(project=project))

    def _check_replicas(self, all_up=True):
        num_replicas = self.bucket.configured_replica_count
        if num_replicas < 1:
            raise SkipTest('need replicas to test get_all/get_any_replicas')
            # TODO: this is far to difficult - having to use the test framework to get the bucket
        kv_results = self.bucket.ping().endpoints.get(ServiceType.KeyValue, None)
        num_expected = num_replicas+1 if all_up else 2 # 2 means at least one replica is up
        if not kv_results or len(kv_results) < num_expected:
            raise SkipTest('not all replicas are online')

    def test_get_any_replica(self):
        self._check_replicas(False)
        self.coll.upsert('imakey100', self.CONTENT)
        result = self.try_n_times(10, 3, self.coll.get_any_replica, 'imakey100')
        self.assertDictEqual(self.CONTENT, result.content_as[dict])

    def test_get_all_replicas(self):
        self._check_replicas()
        self.coll.upsert(self.KEY, self.CONTENT)
        # wait till it it there...
        result = self.try_n_times(10, 3, self.coll.get_all_replicas, self.KEY)
        if not hasattr(result, '__iter__'):
            result = [result]
        for r in result:
            self.assertDictEqual(self.CONTENT, r.content_as[dict])

    def test_get_all_replicas_returns_master(self):
        self._check_replicas()
        self.coll.upsert('imakey100', self.CONTENT)
        result = self.try_n_times(10, 3, self.coll.get_all_replicas, 'imakey100')
        if not hasattr(result, '__iter__'):
            result = [result]
        # TODO: this isn't implemented yet - waiting on CCBC-1169
        # when it does work, we just need to make sure one of the
        # results returns True for is_replica()
        for r in result:
            with self.assertRaises(NotImplementedError):
                r.is_replica()

    def test_touch(self):
        self.cb.touch(self.KEY, timedelta(seconds=3))
        self.try_n_times_till_exception(10, 3, self.cb.get, self.KEY)
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)

    def _authenticator(self):
        if self.is_mock:
            return ClassicAuthenticator(self.cluster_info.admin_username, self.cluster_info.admin_password)
        return PasswordAuthenticator(self.cluster_info.admin_username, self.cluster_info.admin_password)

    def _create_cluster_opts(self, **kwargs):
        return ClusterOptions(self._authenticator(), **kwargs)

    def _mock_hack(self):
        if self.is_mock:
            return {'bucket': self.bucket_name}
        return {}

    def test_get_and_touch(self):
        self.cb.get_and_touch(self.KEY, timedelta(seconds=3))
        self.try_n_times_till_exception(10, 3, self.cb.get, self.KEY)
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)

    def test_get_and_lock(self):
        self.cb.get_and_lock(self.KEY, timedelta(seconds=3))
        self.try_n_times(10, 1, self.cb.upsert, self.KEY, self.CONTENT)
        self.cb.get(self.KEY)

    def test_get_and_lock_upsert_with_cas(self):
        result = self.cb.get_and_lock(self.KEY, timedelta(seconds=15))
        cas = result.cas
        self.assertRaises(DocumentExistsException, self.cb.upsert, self.KEY, self.CONTENT)
        self.cb.replace(self.KEY, self.CONTENT, ReplaceOptions(cas=cas))

    def test_unlock(self):
        cas = self.cb.get_and_lock(self.KEY, timedelta(seconds=15)).cas
        self.cb.unlock(self.KEY, cas)
        self.cb.upsert(self.KEY, self.CONTENT)

    def test_unlock_wrong_cas(self):
        cas = self.cb.get_and_lock(self.KEY, timedelta(seconds=15)).cas
        self.try_n_times_till_exception(10, 1, self.cb.unlock, self.KEY, 100,
                                        expected_exceptions=(TemporaryFailException,))
        self.try_n_times(10, 3, self.cb.unlock, self.KEY, cas, expected_exceptions=(TemporaryFailException,))

    def test_client_durable_upsert(self):
        num_replicas = self.bucket._bucket.configured_replica_count
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        self.cb.upsert(self.NOKEY, self.CONTENT, UpsertOptions(durability=durability))
        result = self.cb.get(self.NOKEY)
        self.assertEqual(self.CONTENT, result.content_as[dict])

    def test_server_durable_upsert(self):
        if not self.supports_sync_durability():
            raise SkipTest("ServerDurability not supported")
        durability = ServerDurability(level=Durability.PERSIST_TO_MAJORITY)
        self.cb.upsert(self.NOKEY, self.CONTENT, UpsertOptions(durability=durability))
        result = self.cb.get(self.NOKEY)
        self.assertEqual(self.CONTENT, result.content_as[dict])

    def test_client_durable_insert(self):
        num_replicas = self.bucket._bucket.configured_replica_count
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        self.cb.insert(self.NOKEY, self.CONTENT, InsertOptions(durability=durability))
        result = self.cb.get(self.NOKEY)
        self.assertEqual(self.CONTENT, result.content_as[dict])

    def test_server_durable_insert(self):
        if not self.supports_sync_durability():
            raise SkipTest("ServerDurability not supported")
        durability = ServerDurability(level=Durability.PERSIST_TO_MAJORITY)
        self.cb.insert(self.NOKEY, self.CONTENT, InsertOptions(durability=durability))
        result = self.cb.get(self.NOKEY)
        self.assertEqual(self.CONTENT, result.content_as[dict])

    def test_client_durable_replace(self):
        num_replicas = self.bucket._bucket.configured_replica_count
        content = {"new":"content"}
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        self.cb.replace(self.KEY, content, ReplaceOptions(durability=durability))
        result = self.cb.get(self.KEY)
        self.assertEqual(content, result.content_as[dict])

    def test_server_durable_replace(self):
        content = {"new":"content"}
        if not self.supports_sync_durability():
            raise SkipTest("ServerDurability not supported")
        durability = ServerDurability(level=Durability.PERSIST_TO_MAJORITY)
        self.cb.replace(self.KEY, content, ReplaceOptions(durability=durability))
        result = self.cb.get(self.KEY)
        self.assertEqual(content, result.content_as[dict])

    @unittest.skip("Client Durable remove not yet supported (CCBC-1199)")
    def test_client_durable_remove(self):
        num_replicas = self.bucket._bucket.configured_replica_count
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        self.cb.remove(self.KEY, RemoveOptions(durability=durability))
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)

    def test_server_durable_remove(self):
        if not self.supports_sync_durability():
            raise SkipTest("ServerDurability not supported")
        durability = ServerDurability(level=Durability.PERSIST_TO_MAJORITY)
        self.cb.remove(self.KEY, RemoveOptions(durability=durability))
        self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)

    def test_collection_access(self,  # type: CollectionTests
                               ):
        if not self.supports_collections():
            raise SkipTest()

        value = uuid.uuid4().int
        value_1 = str(value)
        value_2 = str(value + 1)
        value_3 = str(value + 2)
        value_4 = str(value + 3)
        test_dict = {
            "scope_1": {
                "collection_1": {"key_1": value_1},
                "collection_2": {"key_1": value_2},
            },
            "scope_2": {
                "collection_1": {"key_1": value_3},
                "collection_2": {"key_1": value_4},
            }
        }

        bucket = self.cluster.bucket(self.cluster_info.bucket_name)
        cm = bucket.collections()
        def upsert_values(coll, scope_name, coll_name, result_key_dict, key, value):
            return coll.upsert(key, value)
        from collections import defaultdict
        def recurse():
            return defaultdict(recurse)
        resultdict=recurse()
        def check_values(coll, scope_name, coll_name, result_key_dict, key, value):
            result_key_dict[key]=coll.get(key).content
            return True
        for action in [upsert_values, check_values]:
            self._traverse_scope_tree(bucket, cm, resultdict, test_dict, action)

        self.assertSanitizedEqual(test_dict, resultdict)

    def _traverse_scope_tree(self, bucket, cm, result_dict, test_dict, coll_verb):
        for scope_name, coll_dict in test_dict.items():
            result_coll_dict = result_dict[scope_name]
            logging.error("Creating scope {}".format(scope_name))
            try:
                cm.create_scope(scope_name)
            except:
                pass
            scope = bucket.scope(scope_name)
            for coll_name, key_dict in coll_dict.items():
                result_key_dict = result_coll_dict[coll_name]
                logging.error(
                        "Creating collection {} in scope {}".format(coll_name, scope_name)
                )
                try:
                    cm.create_collection(
                            CollectionSpec(scope_name=scope_name, collection_name=coll_name)
                    )
                except:
                    pass
                coll = scope.collection(coll_name)
                for key, value in key_dict.items():
                    result=coll_verb(coll, scope_name, coll_name, result_key_dict, key, value)
                    logging.error(
                            "Called {} on {} to {} in {} and got {}".format(
                                    coll_verb,
                                    key,
                                    value,
                                    dict(scope_name=scope_name, coll_name=coll_name),
                                    result,
                            )
                    )
