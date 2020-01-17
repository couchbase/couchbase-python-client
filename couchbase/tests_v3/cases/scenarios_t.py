# -*- coding:utf-8 -*-
#
# Copyright 2018, Couchbase, Inc.
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
import traceback
from datetime import timedelta
from typing import *
from unittest import SkipTest

from couchbase.fulltext import SearchResult, MetaData
from couchbase_core import recursive_reload
from couchbase_core._pyport import ANY_STR
import datetime

try:
    from abc import ABC
except:
    from abc import ABCMeta

import logging
import time

from couchbase_core.transcodable import Transcodable

import couchbase_core.connstr
import couchbase.exceptions

from couchbase import JSONDocument, DeltaValue, SignedInt64, MutateInResult
from couchbase_core.durability import Durability
from couchbase import ReplicateTo, PersistTo, copy, \
    timedelta, ReplicaNotConfiguredException, DocumentConcurrentlyModifiedException, \
    DocumentMutationLostException, ReplicaNotAvailableException, MutateSpec, CASMismatchException, \
    MutateInOptions
from couchbase import GetOptions, RemoveOptions, ReplaceOptions
from couchbase import Bucket

from couchbase_tests.base import ClusterTestCase, CollectionTestCase
import couchbase.subdocument as SD
import couchbase.management
import couchbase_core._bootstrap
import couchbase_core._libcouchbase as _LCB
import couchbase_core.cluster
import couchbase_core.tests.analytics_harness
from couchbase.diagnostics import ServiceType
import couchbase_core.fulltext as FT
from couchbase.exceptions import KeyNotFoundException, KeyExistsException, NotSupportedError


class Scenarios(CollectionTestCase):


    def test_scenario_A(self):
        # 1) fetch a full document that is a json document
        self.coll.upsert("id",{"kettle":"fish"})
        doc = self.coll.get("id", GetOptions().timeout(timedelta(seconds=10)))
        # 2) Make a modification to the content
        content = doc.content_as[JSONDocument].put("field", "value")
        # 3) replace the document on the server
        # not formally allowed syntax - can't mix OptionBlocks and named params
        result = self.coll.replace(doc.id, content, ReplaceOptions().timeout(timedelta(seconds=10)), cas=doc.cas)

        result = self.coll.replace(doc.id, content, ReplaceOptions().timeout(timedelta(seconds=10)).cas(result.cas))
        result = self.coll.replace(doc.id, content, expiry=timedelta(seconds=10), cas=result.cas)
        # Default params also supported for all methods
        doc2 = self.coll.get("id", expiry=timedelta(seconds=10))
        content2 = doc2.content_as[dict].update({"value": "bar"})

        self.coll.replace(doc2.id, content2, cas=doc2.cas, expiry=timedelta(seconds=10))

        # I include type annotations and getOrError above to make things clearer,
        # but it'd be more idiomatic to write this:
        try:
            self.coll.get("cheese", GetOptions(replica=True))
            self.coll.get("cheese", replica=True)
            # invalid syntax:
            self.coll.get("cheese", options=GetOptions(replica=True), replica=True)

            result = self.coll.get("id", GetOptions().timeout(timedelta(seconds=10)))
            self.coll.replace(result.id,
                              result.content
                              .put("field", "value")
                              .put("foo", "bar"),
                              cas=result.cas,
                              expiry=timedelta(seconds=10))
        except:
            print("could not get doc")

    def test_scenario_B(self):
        """
          Scenario B:

        1) fetch a document fragment which is a json array with elements
        2) make modifications to the content
        3) replace the fragment in the original document

        """
        self.coll.upsert("id",{'someArray':['wibble','gronk']})
        subdoc = self.coll.get("id", GetOptions(project = ["someArray"]))
        result = None
        if subdoc:
            arr = subdoc.content_as_array()
            arr.append("foo")

            result = self.coll.mutate_in("id", [SD.upsert("someArray", arr)],
                                      MutateInOptions().timeout(timedelta(seconds=10)))

        self.assertIsInstance(result, MutateInResult)
        self.assertEqual('None',result.content_as[str](0))

    from parameterized import parameterized
    @parameterized.expand(
        x for x in tuple(list(Durability._member_names_))
    )
    def test_mutatein(self,  # type: Scenarios
                      dur_name):
        durability=Durability[dur_name]
        count = 0

        somecontents = {'some': {'path': 'keith'}}
        key="{}_{}".format("somekey_{}", count)
        try:
            self.coll.remove(key)
        except:
            pass
        self.coll.insert(key, somecontents)
        inserted_value = "inserted_{}".format(count)
        replacement_value = "replacement_{}".format(count)
        count += 1
        try:
            self.coll.mutate_in(key, (
                SD.replace('some.path', replacement_value),
                SD.insert('some.other.path', inserted_value, create_parents=True),
            ), durability_level=durability)


            somecontents['some']['path'] = replacement_value
            somecontents['some'].update({'other': {'path': inserted_value}})
            self.assertEqual(somecontents, self.coll.get(key).content)
        except NotSupportedError as e:
            if not self.is_mock:
                raise
            else:
                logging.error("Assuming failure is due to mock not supporting durability")
        except couchbase.TimeoutError as e:
            self.assertIn("Operational",e.message)
            raise SkipTest("Raised {}, skipped pending further verification".format(e.message))

    def test_scenario_C_clientSideDurability(self):
        """
        Scenario C:

        1) Remove a document with Durability Requirements, both variants, thinking about error handling
        """

        # Use a helper wrapper to retry our operation in the face of durability failures
        # remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
        # a reasonable restriction.
        self.coll.upsert("id","test")
        self.assertEqual(self.coll.get("id").content_as[str],"test")
        try:
            self.retry_idempotent_remove_client_side(lambda replicateTo:
                                                 self.coll.remove("id",
                                                                  RemoveOptions().dur_client(replicateTo,
                                                                                             PersistTo.ONE)),
                                                     ReplicateTo.TWO, ReplicateTo.TWO, datetime.datetime.now() + timedelta(seconds=30))
        except NotSupportedError:
            raise SkipTest("Skipping as not supported")

    @staticmethod
    def retry_idempotent_remove_client_side(callback,  # type: Callable[[ReplicateTo.Value],Any]
                                            replicate_to,  # type: ReplicateTo.Value
                                            original_replicate_to,  # type: ReplicateTo.Value
                                            until  # type: datetime.datetime
                                            ):
        # type: (...) -> None
        """
          * Automatically retries an idempotent operation in the face of durability failures
          * TODO this is quite complex logic.  Should this be folded into the client as a per-operation retry strategy?
          * @param callback an idempotent remove operation to perform
          * @param replicate_to the current ReplicateTo setting being tried
          * @param original_replicate_to the originally requested ReplicateTo setting
          * @param until prevent the operation looping indefinitely
          """
        success = False
        while not success:
            if datetime.datetime.now() >= until:
                # Depending on the durability requirements, may want to also log this to an external system for human review
                # and reconciliation
                raise RuntimeError("Failed to durably write operation")

            try:
                callback(replicate_to)
                success = True
            except couchbase.exceptions.KeyNotFoundException:
                print("Our work here is done")
                break

            except ReplicaNotConfiguredException:
                print("Not enough replicas configured, aborting")
                break

            except DocumentConcurrentlyModifiedException:
                # Just retry
                # self.retryIdempotentRemoveClientSide(callback, replicate_to, original_replicate_to, until)
                continue

            except DocumentMutationLostException:
                # Mutation lost during a hard failover. If enough replicas
                # still aren't available, it will presumably raise ReplicaNotAvailableException and retry with lower.
                # self.retryIdempotentRemoveClientSide(callback, original_replicate_to, original_replicate_to, until)
                replicate_to = original_replicate_to
                continue

            except (ReplicaNotAvailableException, couchbase.ArgumentError):
                newReplicateTo = {ReplicateTo.ONE: ReplicateTo.NONE,
                                  ReplicateTo.TWO: ReplicateTo.ONE,
                                  ReplicateTo.THREE: ReplicateTo.TWO}.get(replicate_to, ReplicateTo.NONE)
                print("Temporary replica failure, retrying with lower durability {}".format(newReplicateTo))
                replicate_to = newReplicateTo

    def test_scenario_c_server_side_durability(self):
        # Use a helper wrapper to retry our operation in the face of durability failures
        # remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
        # a reasonable restriction.
        for durability_type in Durability:
            self.coll.upsert("id","fred",durability_level=Durability.NONE)
            self.retry_idempotent_remove_server_side(
                lambda: self.coll.remove("id", RemoveOptions().dur_server(durability_type)))

    def retry_idempotent_remove_server_side(self,  # type: Scenarios
                                            callback,  # type: Callable[[],Any]
                                            until=timedelta(seconds=10)  # type: timedelta
                                            ):
        """
          * Automatically retries an idempotent operation in the face of durability failures
          * TODO Should this be folded into the client as a per-operation retry strategy?
          * @param callback an idempotent remove operation to perform
          * @param until prevent the operation looping indefinitely
          */"""
        deadline=datetime.datetime.now()+until
        while datetime.datetime.now() < deadline:

            try:
                callback()
                return
            except couchbase.exceptions.DurabilitySyncWriteAmbiguousException:
                if self.coll.get("id").success:
                    continue
                logging.info("Our work here is done")
                return

            except couchbase.exceptions.KeyNotFoundException:
                logging.info("Our work here is done")
                return
        # Depending on the durability requirements, may want to also log this to an external system for human review
        # and reconciliation
        raise RuntimeError("Failed to durably write operation")

    def test_scenario_D(self):
        """  Scenario D (variation of A):

        #1) do the same thing as A, but handle the "cas mismatch retry loop"
        """
        entry=JSONDocument()
        entry=entry.put("field","value")
        self.coll.upsert("id",entry)
        def respond():
            result = self.coll.get("id", expiry=timedelta(seconds=10))
            if result:
                self.coll.replace(result.id,
                                  result.content_as[JSONDocument]
                                  .put("field", "value")
                                  .put("foo", "bar"),
                                  cas=result.cas,
                                  expiry=timedelta(seconds=10))
            else:
                logging.error("could not get doc")

        self.retry_operation_on_cas_mismatch(respond, guard=50)

    def retry_operation_on_cas_mismatch(self,
                                        callback,  # type: Callable[[],None]
                                        guard  # type: int
                                        ):
        # type: (...) -> None
        if guard <= 0:
            raise RuntimeError("Failed to perform exception")

        try:
            callback()
        except CASMismatchException:
            self.retry_operation_on_cas_mismatch(callback, guard - 1)

    class UserPartial(Transcodable):
        def __init__(self,
                     name=None,  # type: str
                     age=None,  # type: int
                     **kwargs
                     ):
            self.name = name
            self.age = age

        def with_attr(self, **kwargs):
            result = copy.deepcopy(self)
            for k, v in kwargs.items():
                setattr(result, k, v)
            return result

        def __eq__(self, other):
            return self.name==other.name and self.age==other.age

        @classmethod
        def decode_canonical(cls, input):
            return cls(**input)

        def encode_canonical(self):
            return dict(name=self.name,age=self.age)

    class AddressedUser(UserPartial):
        def __init__(self,
                     name=None,  # type: str
                     age=None,  # type: int
                     address=None,  # type: str
                     ):
            super(Scenarios.AddressedUser,self).__init__(name,age)
            self.address = address

        def __eq__(self, other):
            return super(Scenarios.AddressedUser,self).__eq__(other) and self.address==other.address

        def encode_canonical(self):
            result=super(Scenarios.AddressedUser,self).encode_canonical()
            result.update(address=self.address)
            return result

    class PhonedUser(AddressedUser):
        def __init__(self,
                     name=None,  # type: str
                     age=None,  # type: int
                     address=None,  # type: str
                     phoneNumber=None  # type: str
                     ):
            super(Scenarios.PhonedUser,self).__init__(name,age,address)
            self.phoneNumber = phoneNumber

        def __eq__(self, other):
            return super(Scenarios.AddressedUser,self).__eq__(other) and self.address==other.address

        def encode_canonical(self):
            result=super(Scenarios.AddressedUser,self).encode_canonical()
            result.update(phoneNumber=self.phoneNumber)
            return result

    def test_scenario_E(self):
        """
              Scenario E (if applicable):

        1) Fetch a full Document and marshal it into a language entity rather than a generic json type
        2) Modify the entity
        3) store it back on the server with a replace
        """
        self.coll.upsert("id",dict(name="fred"))
        result = self.coll.get("id", expiry=timedelta(seconds=10))
        if result:
            entry = result.content_as[Scenarios.AddressedUser]
            entry=entry.with_attr(age=25)
            self.coll.replace(result.id, entry, cas=result.cas, expiry=timedelta(seconds=10))
        else:
            logging.error("could not get doc")

    def test_scenario_F_fulldoc(self):
        """
              Scenario F (if applicable):
        1) Fetch a Document fragment and marshal it into a language entity rather than a generic json type
        2) Modify the entity
        3) store it back on the server with a replace
        """

        item=Scenarios.AddressedUser("fred",21,"45 Dupydaub Street")
        self.coll.upsert("id",item)
        doc = self.coll.get("id")
        if doc:
            result = doc.content_as[Scenarios.AddressedUser]
            self.assertEqual(result,item)
            result=result.with_attr(age=25)
            self.assertNotEqual(result,item)
        else:
            logging.error("could not find doc")

    def test_scenarioF_subdoc(self):

        item=Scenarios.AddressedUser("fred",21,"45 Dupydaub Street")
        self.coll.upsert("id", item)
        subdoc = self.coll.get("id", project=("name", "age"))

        user = subdoc.content_as[Scenarios.UserPartial]
        altuser=self.coll.lookup_in("id", (SD.get("name"), SD.get("age")))
        self.assertEqual("fred",altuser.content_as[str](0))
        self.assertEqual(21,altuser.content_as[int](1))
        changed = user.with_attr(age=25)
        self.assertEqual(Scenarios.UserPartial("fred", 25), changed)

        self.coll.mutate_in(subdoc.id, [MutateSpec().upsert("user", changed)])

    def test_upsert(self):

        self.coll.upsert("fish", "banana")
        self.assertEqual("banana", self.coll.get("fish").content_as[str])

    def test_unsigned_int(self):
        self.assertRaises(couchbase.exceptions.ArgumentError, DeltaValue, -1)
        self.assertRaises(couchbase.exceptions.ArgumentError, DeltaValue, 0x7FFFFFFFFFFFFFFF + 1)
        x=DeltaValue(5)
        self.assertEqual(5,x.value)

    def test_signed_int_64(self):
        self.assertRaises(couchbase.exceptions.ArgumentError, SignedInt64, -0x7FFFFFFFFFFFFFFF-2)
        self.assertRaises(couchbase.exceptions.ArgumentError, SignedInt64, 0x7FFFFFFFFFFFFFFF + 1)
        x=SignedInt64(0x7FFFFFFFFFFFFFFF)
        self.assertEqual(0x7FFFFFFFFFFFFFFF,x.value)
        x=SignedInt64(-0x7FFFFFFFFFFFFFFF-1)
        self.assertEqual(-0x7FFFFFFFFFFFFFFF-1,x.value)

    def test_decrement(self):
        try:
            self.coll.remove("counter")
        except:
            pass
        self.coll.decrement("counter", DeltaValue(0), initial=SignedInt64(43))
        self.assertEqual(43,self.coll.get("counter").content_as[int])
        self.coll.decrement("counter", DeltaValue(1))
        self.assertEqual(42,self.coll.get("counter").content_as[int])

        self.coll.remove("counter")
        self.coll.upsert("counter", 43)
        self.coll.decrement("counter", DeltaValue(1))
        self.assertEqual(42,self.coll.get("counter").content_as[int])

        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.decrement, "counter", DeltaValue(5), initial=10)
        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.decrement, "counter", 5)
        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.decrement, "counter",-3)

    def test_increment(self):
        try:
            self.coll.remove("counter",quiet=True)
        except:
            pass
        self.coll.increment("counter", DeltaValue(0), initial=SignedInt64(43))
        self.assertEqual(43,self.coll.get("counter").content_as[int])
        self.coll.increment("counter", DeltaValue(1))
        self.assertEqual(44,self.coll.get("counter").content_as[int])

        self.coll.remove("counter")
        self.coll.upsert("counter", 43)
        self.coll.increment("counter", DeltaValue(1))
        self.assertEqual(44,self.coll.get("counter").content_as[int])

        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.increment, "counter", DeltaValue(5), initial=10)
        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.increment, "counter", 5)
        self.assertRaises(couchbase.exceptions.ArgumentError, self.coll.increment, "counter", -3)

    def test_cluster_query(self):
        if self.is_mock:
          raise SkipTest("Query not supported in mock")
        result = self.cluster.query("SELECT * from `beer-sample` LIMIT 1")
        self.assertIsNotNone(result)
        count = 0
        for row in result.rows():
          count += 1
        self.assertEquals(1, count)

    def test_cluster_search(self  # type: ClusterTestCase
                            ):
        if self.is_mock:
            raise SkipTest("F.T.S. not supported by mock")
        most_common_term_max = 10
        initial=time.time()
        x = self.cluster.search_query("beer-search", FT.TermQuery("category"),
                                      facets={'fred': FT.TermFacet('category', most_common_term_max)})
        first_entry = x.hits()[0]
        self.assertEqual("brasserie_de_brunehaut-mont_st_aubert", first_entry.get('id'))
        min_hits = 6
        metadata = x.metadata()
        duration=time.time()-initial
        self.assertIsInstance(metadata, MetaData)
        self.assertIsInstance(metadata.error_count(), int)
        self.assertIsInstance(metadata.max_score(), float)
        self.assertIsInstance(metadata.success_count(), int)
        took=metadata.took()
        self.assertIsInstance(took, timedelta)
        self.assertAlmostEqual(took.value, duration, delta=0.1)
        self.assertGreater(took.value, 0)
        self.assertIsInstance(metadata.total_hits(), int)
        self.assertGreaterEqual(metadata.success_count(), min_hits)
        self.assertGreaterEqual(metadata.total_hits(), min_hits)
        self.assertGreaterEqual(len(x.hits()), min_hits)
        fred_facet = x.facets()['fred']
        self.assertIsInstance(fred_facet, SearchResult.Facet)
        self.assertEqual(len(fred_facet['terms']), most_common_term_max)

        self.assertRaises(couchbase.exceptions.SearchException, self.cluster.search_query, "beer-search",
                          FT.TermQuery("category"),
                          facets={'fred': None})

    def test_diagnostics(self  # type: Scenarios
                         ):
        try:
            diagnostics = self.cluster.diagnostics(timeout=(5))
        except couchbase.exceptions.TimeoutError:
            if self.is_mock:
                raise SkipTest("LCB Diagnostics still blocks indefinitely with mock: {}".format(traceback.format_exc()))

        self.assertRegex(diagnostics.sdk(), r'.*PYCBC.*')
        self.assertGreaterEqual(diagnostics.version(), 1)
        self.assertIsNotNone(diagnostics.id())
        config = diagnostics.services().get('config')
        self.assertEqual(config.type(), ServiceType.Config)
        for key, value in diagnostics.services().items():
            self.assertIn(type(value.type()), (ServiceType, str))
            self.assertIn(type(value.id()), ANY_STR)
            self.assertIn(type(value.local()), ANY_STR)

    @staticmethod
    def get_multi_result_as_dict(result):
        return {k: v.content for k, v in result.items()}

    @staticmethod
    def get_multi_mutationresult_as_dict(result):
        return {k: v.success() for k, v in result.items()}

    def test_multi(self):
        test_dict = {"Fred": "Wilma", "Barney": "Betty"}
        for dur_level in [Durability.NONE] if self.is_mock else Durability:
            try:
                self.coll.remove_multi(test_dict.keys())
            except:
                pass
            mutate_kwargs = dict(durability_level=dur_level)
            self.assertRaises(KeyNotFoundException, self.coll.get, "Fred")
            self.assertRaises(KeyNotFoundException, self.coll.get, "Barney")
            self.coll.upsert_multi(test_dict, **mutate_kwargs)
            result = self.coll.get_multi(test_dict.keys())
            self.assertEqual(Scenarios.get_multi_result_as_dict(result), test_dict)
            self.coll.remove_multi(test_dict.keys(), **mutate_kwargs)
            self.assertRaises(KeyNotFoundException, self.coll.get_multi, test_dict.keys())
            self.coll.insert_multi(test_dict, **mutate_kwargs)
            self.assertRaises(KeyExistsException, self.coll.insert_multi, test_dict)
            result = self.coll.get_multi(test_dict.keys())
            self.assertEqual(Scenarios.get_multi_result_as_dict(result), test_dict)
            self.assertEqual(self.coll.get("Fred").content, "Wilma")
            self.assertEqual(self.coll.get("Barney").content, "Betty")
            self.coll.remove_multi(test_dict.keys(), **mutate_kwargs)
            self.assertRaises(KeyNotFoundException, self.coll.get_multi, test_dict.keys())
            self.coll.insert_multi(test_dict)
            test_dict_2 = {"Fred": "Cassandra", "Barney": "Raquel"}
            result = self.coll.replace_multi(test_dict_2)
            expected_result = {k: True for k, v in test_dict_2.items()}
            self.assertEqual(Scenarios.get_multi_mutationresult_as_dict(result), expected_result)
            self.assertEqual(Scenarios.get_multi_result_as_dict(self.coll.get_multi(test_dict_2.keys())),test_dict_2)

    def test_PYCBC_607(self  # type: Scenarios
                       ):
        messed_helpers = copy.deepcopy(couchbase_core._bootstrap._default_helpers)

        def dummy_call(*args, **kwargs):
            raise Exception("failed")

        messed_helpers['json_encode'] = dummy_call
        messed_helpers['pickle_encode'] = dummy_call
        _LCB._init_helpers(**messed_helpers)

        def do_upsert():
            self.coll.upsert('king_arthur', {'name': 'Arthur', 'email': 'kingarthur@couchbase.com',
                                             'interests': ['Holy Grail', 'African Swallows']})

        self.assertRaises(Exception, do_upsert)
        recursive_reload(couchbase)
        do_upsert()

    def test_datastructures(self  # type: Scenarios
                            ):
        self.coll.upsert("Fred", {"cheese": "potato"})
        self.coll.map_add("Fred", "Gail", "Porter")
        self.assertEqual("Porter", self.coll.map_get("Fred", "Gail"))

class AnalyticsTest(couchbase_core.tests.analytics_harness.CBASTestSpecific, ClusterTestCase):
    def setUp(self):
        self.factory=Bucket
        ClusterTestCase.setUp(self)
        couchbase_core.tests.analytics_harness.CBASTestSpecific.setUp(self)

    def get_fixture(self):
        return self.cluster

    def do_analytics_query(self, query, **kwargs):
        return self.cluster.analytics_query(query, **kwargs)
