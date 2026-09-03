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

# Two suites, because the file covers two APIs.  `DatastructuresTestSuite` exercises the
# objects reached through `collection.couchbase_list(key)` and friends;
# `LegacyDatastructuresTestSuite` exercises `collection.list_append(...)` and its three
# siblings, which exist only on the sync Collection and are deprecated for removal in 4.7.0
# under PYCBC-1732.  Generated for acouchbase only: txcouchbase has no datastructure
# support at all.
#
# Every test starts from a key that must not exist, because the datastructure operations
# create the document on first write.  A fixed key would be a literal in the shared
# `_default` collection, which the DEFAULT strategy isolates by UUID-prefixed key precisely
# because other pytest workers write there at the same time.

from __future__ import annotations

from collections.abc import Generator

import pytest

from couchbase.datastructures import (CouchbaseList,
                                      CouchbaseMap,
                                      CouchbaseQueue,
                                      CouchbaseSet)
from couchbase.exceptions import (DocumentNotFoundException,
                                  InvalidArgumentException,
                                  QueueEmpty)
from couchbase.result import OperationResult
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    TestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest


class DatastructuresTestSuite(SuiteRequirements):
    # Every key comes from get_new_doc(), which mints one the collection has never held.
    NUM_DOCS = 0

    TEST_MANIFEST = [
        'test_list',
        'test_map',
        'test_queue',
        'test_sets',
    ]

    def test_list(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)
        cb_list = cb_env.collection.couchbase_list(key)
        assert isinstance(cb_list, CouchbaseList)

        cb_list.append('world')
        rv = cb_list.get_at(0)
        assert str(rv) == 'world'

        cb_list.prepend('hello')
        rv = cb_list.get_at(0)
        assert str(rv) == 'hello'

        rv = cb_list.get_at(1)
        assert str(rv) == 'world'
        assert 2 == cb_list.size()

        cb_list.remove_at(1)
        res = cb_env.collection.get(key)
        assert ['hello'] == res.content_as[list]

        cb_list.append('world')
        res = cb_env.collection.get(key)
        assert ['hello', 'world'] == res.content_as[list]

        cb_list.set_at(1, 'after')
        res = cb_env.collection.get(key)
        assert ['hello', 'after'] == res.content_as[list]

        assert ['hello', 'after'] == cb_list.get_all()
        assert 1 == cb_list.index_of('after')

        # Collected, then compared as a sequence.  Asserting inside the loop instead leaves the
        # whole check satisfied by an iterator that yields nothing, and this loop is the only
        # place iteration is exercised.
        seen: list[str] = []
        for v in cb_list:
            seen.append(v)
        assert ['hello', 'after'] == seen

        cb_list.clear()

        assert 0 == cb_list.size()

    def test_map(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)
        cb_map = cb_env.collection.couchbase_map(key)
        assert isinstance(cb_map, CouchbaseMap)

        cb_map.add('key1', 'val1')

        rv = cb_map.get('key1')
        assert rv == 'val1'

        assert 1 == cb_map.size()

        with pytest.raises(InvalidArgumentException):
            cb_map.remove('key2')
        # A remove that raises has to leave the document alone.  Checked because the call
        # above reaches the server: it is a subdoc mutation that fails on a missing path, not
        # an argument rejected before any I/O.
        assert 1 == cb_map.size()

        cb_map.add('key2', 'val2')

        keys = cb_map.keys()
        assert ['key1', 'key2'] == keys

        values = cb_map.values()
        assert ['val1', 'val2'] == values

        assert cb_map.exists('key1') is True
        assert cb_map.exists('no-key') is False

        assert {'key1': 'val1', 'key2': 'val2'} == cb_map.get_all()

        # keys() and values() are already compared exactly, so what items() adds is the pairing
        # between them.  Two membership checks cannot see that: swap the values and `k in
        # expected_keys` / `v in expected_values` both still hold.
        assert [('key1', 'val1'), ('key2', 'val2')] == list(cb_map.items())

        # The map deliberately supports no iteration of its own: keys(), values() and items()
        # are the interface, so the loop body is unreachable.  The suppression is the more
        # useful half of the check, because warn_unused_ignores turns it into a tripwire:
        # give CouchbaseMap an __iter__ and this ignore goes unused and fails the build.
        with pytest.raises(TypeError):
            for _ in cb_map:  # type: ignore[attr-defined]
                pass

        cb_map.remove('key1')
        assert 1 == cb_map.size()
        # Which key survived, not just how many.  size() alone is satisfied by a remove that
        # took the wrong entry.
        assert ['key2'] == cb_map.keys()

        cb_map.clear()
        assert 0 == cb_map.size()

    def test_queue(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)
        cb_queue = cb_env.collection.couchbase_queue(key)
        assert isinstance(cb_queue, CouchbaseQueue)

        cb_queue.push(1)
        cb_queue.push(2)
        cb_queue.push(3)

        assert cb_queue.pop() == 1
        assert cb_queue.pop() == 2
        assert cb_queue.pop() == 3
        with pytest.raises(QueueEmpty):
            cb_queue.pop()

        cb_queue.push(1)
        cb_queue.push(2)
        cb_queue.push(3)

        assert cb_queue.size() == 3

        # Same reason as test_list: an empty iterator has to be able to fail this.  A queue pops
        # oldest-first, so iteration is the reverse of the push order.
        seen: list[int] = []
        for v in cb_queue:
            seen.append(v)
        assert [3, 2, 1] == seen

        cb_queue.clear()

        assert 0 == cb_queue.size()

    def test_sets(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)
        cb_set = cb_env.collection.couchbase_set(key)
        assert isinstance(cb_set, CouchbaseSet)

        # add() reports whether it added the value or found it already there, which is the
        # whole difference between these two calls and is visible only if the return is read.
        assert cb_set.add(123) is True
        assert cb_set.add(123) is False
        assert 1 == cb_set.size()
        assert cb_set.contains(123) is True

        cb_set.remove(123)
        assert 0 == cb_set.size()
        # remove() is annotated `-> None` and returns None on every path, so the claim worth
        # making is about the effect: removing a value the set does not hold is a no-op.
        cb_set.remove(123)
        assert 0 == cb_set.size()
        assert cb_set.contains(123) is False

        cb_set.add(1)
        cb_set.add(2)
        cb_set.add(3)
        cb_set.add(4)

        values = cb_set.values()
        assert values == [1, 2, 3, 4]
        cb_set.clear()
        assert 0 == cb_set.size()


# Excluded from generation as a whole class.  Per-test exclusion would leave the class and its
# concrete counterpart behind with an empty manifest, which is dead code in the generated file
# rather than an accurate statement that the api has nothing to test here.
@pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                             reason='list_/map_/set_/queue_ exist only on the sync Collection')
class LegacyDatastructuresTestSuite(SuiteRequirements):
    # Kept until the methods are.  Every one of them carries a deprecation warning naming the
    # object-API replacement DatastructuresTestSuite above covers, so when the removal lands
    # this class and its concrete subclass are deleted alongside them.

    NUM_DOCS = 0

    TEST_MANIFEST = [
        'test_list',
        'test_map',
        'test_queue',
        'test_sets',
    ]

    def test_list(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.list_append(key, 'world')

        cb_env.collection.list_append(key, 'world', create=True)
        rv = cb_env.collection.list_get(key, 0)
        assert str(rv) == 'world'

        cb_env.collection.list_prepend(key, 'hello')
        rv = cb_env.collection.list_get(key, 0)
        assert str(rv) == 'hello'
        rv = cb_env.collection.list_get(key, 1)
        assert str(rv) == 'world'
        assert 2 == cb_env.collection.list_size(key)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.list_get(TestEnvironment.NOT_A_KEY, 0)

        cb_env.collection.list_remove(key, 1)
        res = cb_env.collection.get(key)
        assert ['hello'] == res.content_as[list]

        cb_env.collection.list_append(key, 'world')
        res = cb_env.collection.get(key)
        assert ['hello', 'world'] == res.content_as[list]

        cb_env.collection.list_set(key, 1, 'after')
        res = cb_env.collection.get(key)
        assert ['hello', 'after'] == res.content_as[list]

    def test_map(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.map_add(key, 'key1', 'val1')

        cb_env.collection.map_add(key, 'key1', 'val1', create=True)

        rv = cb_env.collection.map_get(key, 'key1')
        assert rv == 'val1'

        assert 1 == cb_env.collection.map_size(key)

        # IndexError, not a Couchbase exception and not the KeyError a mapping would raise.  It
        # comes from the deprecated methods' shared error wrapper, which turns a missing subdoc
        # path into IndexError whatever the path meant.  The object API raises
        # InvalidArgumentException for the same call; see test_map above.
        with pytest.raises(IndexError):
            cb_env.collection.map_remove(key, 'key2')

        cb_env.collection.map_remove(key, 'key1')
        assert 0 == cb_env.collection.map_size(key)

    def test_queue(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.queue_push(key, 1)

        rv = cb_env.collection.queue_push(key, 1, create=True)
        assert isinstance(rv, OperationResult)
        cb_env.collection.queue_push(key, 2)
        cb_env.collection.queue_push(key, 3)

        assert cb_env.collection.queue_pop(key) == 1
        assert cb_env.collection.queue_pop(key) == 2
        assert cb_env.collection.queue_pop(key) == 3
        with pytest.raises(QueueEmpty):
            cb_env.collection.queue_pop(key)

    def test_sets(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.set_add(key, 123)

        rv = cb_env.collection.set_add(key, 123, create=True)
        assert isinstance(rv, OperationResult)
        # Unlike CouchbaseSet.add, which returns False, the deprecated method returns None when
        # the value is already present, so this assertion does distinguish the two calls.
        rv = cb_env.collection.set_add(key, 123)
        assert rv is None
        assert 1 == cb_env.collection.set_size(key)
        assert cb_env.collection.set_contains(key, 123) is True

        rv = cb_env.collection.set_remove(key, 123)
        assert 0 == cb_env.collection.set_size(key)
        rv = cb_env.collection.set_remove(key, 123)
        assert rv is None
        assert cb_env.collection.set_contains(key, 123) is False


class DatastructuresTests(DatastructuresTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(DatastructuresTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, DatastructuresTestSuite, request.param)
        yield env
        teardown_environment(env, DatastructuresTestSuite, request.param)


@pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                             reason='concrete class for a suite excluded from generation')
class LegacyDatastructuresTests(LegacyDatastructuresTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(LegacyDatastructuresTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, LegacyDatastructuresTestSuite, request.param)
        yield env
        teardown_environment(env, LegacyDatastructuresTestSuite, request.param)
