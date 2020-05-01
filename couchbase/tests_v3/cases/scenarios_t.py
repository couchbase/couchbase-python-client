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
import datetime
from typing import *
from unittest import SkipTest

from couchbase_core import recursive_reload

try:
    from abc import ABC
except:
    from abc import ABCMeta

import logging

from couchbase_core.transcodable import Transcodable

import couchbase_core.connstr
import couchbase.exceptions

from couchbase.JSONdocument import JSONDocument
import copy
from datetime import timedelta
from couchbase.subdocument import MutateSpec
from couchbase.exceptions import  ReplicaNotConfiguredException, DocumentConcurrentlyModifiedException, \
    DocumentMutationLostException, ReplicaNotAvailableException,CASMismatchException
from couchbase.collection import GetOptions, RemoveOptions, ReplaceOptions, MutateInOptions
from six import raise_from
from couchbase.result import MutateInResult
from couchbase_tests.base import CollectionTestCase
import couchbase.subdocument as SD
import couchbase.management
import couchbase_core._bootstrap
import couchbase_core._libcouchbase as _LCB
import couchbase_core.tests.analytics_harness
from couchbase.exceptions import DocumentNotFoundException, DocumentExistsException, NotSupportedException
from couchbase.durability import ClientDurability, ServerDurability, DurabilityOptionBlock, Durability, \
    PersistTo, ReplicateTo


class Scenarios(CollectionTestCase):
    def test_scenario_A(self):
        # 1) fetch a full document that is a json document
        self.coll.upsert("id",{"kettle":"fish"})
        doc = self.coll.get("id", GetOptions(timeout=timedelta(seconds=10)))
        # 2) Make a modification to the content
        content = doc.content_as[JSONDocument].put("field", "value")
        # 3) replace the document on the server
        # not formally allowed syntax - can't mix OptionBlocks and named params
        result = self.coll.replace(doc.id, content, ReplaceOptions(timeout=timedelta(seconds=10)), cas=doc.cas)

        result = self.coll.replace(doc.id, content, ReplaceOptions(timeout=timedelta(seconds=10), cas=result.cas))
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

            result = self.coll.get("id", GetOptions(timeout=timedelta(seconds=10)))
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
        self.coll.upsert("id", {'someArray': ['wibble', 'gronk']})
        subdoc = self.coll.get("id", GetOptions(project=["someArray"]))
        result = None
        if subdoc:
            arr = subdoc.content_as[list]
            arr.append("foo")

            result = self.coll.mutate_in("id", [SD.upsert("someArray", arr)],
                                      MutateInOptions(timeout=timedelta(seconds=10)))

        self.assertIsInstance(result, MutateInResult)
        self.assertEqual('None',result.content_as[str](0))

    from parameterized import parameterized

    @parameterized.expand(
        x for x in tuple(list(Durability._member_names_))
    )
    def test_mutatein(self,  # type: Scenarios
                      dur_name):
        durability=Durability[dur_name]
        dur_option = DurabilityOptionBlock(durability=ServerDurability(level=durability))
        count = 0
        replica_count = self.bucket._bucket.configured_replica_count
        if dur_name != Durability.NONE and (replica_count == 0 or self.is_mock):
            raise SkipTest("cluster will not support {}".format(dur_name))
        if not self.supports_sync_durability():
            dur_option = self.sdk3_to_sdk2_durability(dur_name, replica_count)

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
            ), dur_option)


            somecontents['some']['path'] = replacement_value
            somecontents['some'].update({'other': {'path': inserted_value}})
            self.assertEqual(somecontents, self.coll.get(key).content)
        except NotSupportedException as e:
            if not self.is_mock:
                raise
            else:
                logging.error("Assuming failure is due to mock not supporting durability")
        except couchbase.exceptions.TimeoutException as e:
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
                                                                  RemoveOptions(durability=ClientDurability(replicateTo,
                                                                                                            PersistTo.ONE))),
                                                     ReplicateTo.TWO, ReplicateTo.TWO, datetime.datetime.now() + timedelta(seconds=30))
        except NotSupportedException as f:
            raise SkipTest("Using a ClientDurability should work, but it doesn't: {}".format(str(f)))


    def retry_idempotent_remove_client_side(self,
                                            callback,  # type: Callable[[ReplicateTo],Any]
                                            replicate_to,  # type: ReplicateTo
                                            original_replicate_to,  # type: ReplicateTo
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
            except couchbase.exceptions.DocumentNotFoundException:
                print("Our work here is done")
                break

            except ReplicaNotConfiguredException as e:
                print("Not enough replicas configured, aborting")
                if self.is_mock:
                    raise_from(NotSupportedException("Not enough replicas configured, aborting"), e)
                else:
                    raise

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

            except (ReplicaNotAvailableException) as e:
                newReplicateTo = {ReplicateTo.ONE: ReplicateTo.NONE,
                                  ReplicateTo.TWO: ReplicateTo.ONE,
                                  ReplicateTo.THREE: ReplicateTo.TWO}.get(replicate_to, ReplicateTo.NONE)
                print("Temporary replica failure [{}], retrying with lower durability {}".format(str(e), newReplicateTo))
                replicate_to = newReplicateTo

    def test_scenario_c_server_side_durability(self):
        # Use a helper wrapper to retry our operation in the face of durability failures
        # remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
        # a reasonable restriction.
        for durability_type in Durability:
            self.coll.upsert("id","fred",durability=ServerDurability(Durability.NONE))
            self.retry_idempotent_remove_server_side(
                lambda: self.coll.remove("id", RemoveOptions(durability=ServerDurability(durability_type))))

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

            except couchbase.exceptions.DocumentNotFoundException:
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

    def test_cluster_query(self):
        if self.is_mock:
          raise SkipTest("Query not supported in mock")
        result = self.cluster.query("SELECT * from `beer-sample` LIMIT 1")
        self.assertIsNotNone(result)
        count = 0
        for row in result.rows():
          count += 1
        self.assertEquals(1, count)

    @staticmethod
    def get_multi_result_as_dict(result):
        return {k: v.content for k, v in result.items()}

    @staticmethod
    def get_multi_mutationresult_as_dict(result):
        return {k: v.success for k, v in result.items()}

    @staticmethod
    def sdk3_to_sdk2_durability(durability, num_replicas):
        if durability == Durability.NONE:
            return ClientDurability(PersistTo.NONE, ReplicateTo.NONE)
        if durability == Durability.MAJORITY:
            return ClientDurability(replicate_to=ReplicateTo(int((num_replicas+1)/2)), persist_to=PersistTo.NONE)
        if durability == Durability.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return ClientDurability(replicate_to=ReplicateTo(int((num_replicas+1)/2)), persist_to=PersistTo.ONE)
        if durability == Durability.PERSIST_TO_MAJORITY:
            return ClientDurability(persist_to=PersistTo(int((num_replicas+1)/2 + 1)), replicate_to=ReplicateTo.NONE)

    def test_multi(self):
        test_dict = {"Fred": "Wilma", "Barney": "Betty"}
        # TODO: rewrite these tests to test one thing at a time, if possible
        try:
            self.coll.remove_multi(test_dict.keys())
        except DocumentNotFoundException:
            pass
        self.assertRaises(DocumentNotFoundException, self.coll.get, "Fred")
        self.assertRaises(DocumentNotFoundException, self.coll.get, "Barney")
        self.coll.upsert_multi(test_dict)
        result = self.coll.get_multi(test_dict.keys())
        self.assertEqual(Scenarios.get_multi_result_as_dict(result), test_dict)
        self.coll.remove_multi(test_dict.keys())
        self.assertRaises(DocumentNotFoundException, self.coll.get_multi, test_dict.keys())
        self.coll.insert_multi(test_dict)
        self.assertRaises(DocumentExistsException, self.coll.insert_multi, test_dict)
        result = self.coll.get_multi(test_dict.keys())
        self.assertEqual(Scenarios.get_multi_result_as_dict(result), test_dict)
        self.assertEqual(self.coll.get("Fred").content, "Wilma")
        self.assertEqual(self.coll.get("Barney").content, "Betty")
        self.coll.remove_multi(test_dict.keys())
        self.assertRaises(DocumentNotFoundException, self.coll.get_multi, test_dict.keys())
        self.coll.insert_multi(test_dict)
        test_dict_2 = {"Fred": "Cassandra", "Barney": "Raquel"}
        result = self.coll.replace_multi(test_dict_2)
        expected_result = {k: True for k, v in test_dict_2.items()}
        self.assertEqual(Scenarios.get_multi_mutationresult_as_dict(result), expected_result)
        self.assertEqual(Scenarios.get_multi_result_as_dict(self.coll.get_multi(test_dict_2.keys())), test_dict_2)

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

