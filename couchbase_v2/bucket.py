#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import couchbase_core._bootstrap
import couchbase_core._libcouchbase as _LCB
from couchbase_core._libcouchbase import Bucket as _Base

import couchbase_v2
from couchbase_core.bucket import Bucket as CoreBucket, _depr
from couchbase_core.analytics import AnalyticsQuery
from couchbase_v2.exceptions_shim import *
from couchbase_core.result import *
from couchbase_core.bucketmanager import BucketManager

import couchbase_core.fulltext as _FTS
import couchbase_core.subdocument as SD
import json
from typing import *


class Pipeline(object):
    def __init__(self, parent):
        """
        .. versionadded:: 1.2.0

        Creates a new pipeline context. See :meth:`~Bucket.pipeline`
        for more details
        """
        self._parent = parent
        self._results = None

    def __enter__(self):
        self._parent._pipeline_begin()

    def __exit__(self, *args):
        self._results = self._parent._pipeline_end()
        return False

    @property
    def results(self):
        """
        Contains a list of results for each pipelined operation
        executed within the context. The list remains until this
        context is reused.

        The elements in the list are either :class:`.Result`
        objects (for single operations) or :class:`.MultiResult`
        objects (for multi operations)
        """
        return self._results


class DurabilityContext(object):
    def __init__(self, parent, persist_to=-1, replicate_to=-1, timeout=0.0):
        self._parent = parent
        self._new = {
            '_dur_persist_to': persist_to,
            '_dur_replicate_to': replicate_to,
            '_dur_timeout': int(timeout * 1000000)
        }
        self._old = {}

    def __enter__(self):
        for k, v in self._new.items():
            self._old[k] = getattr(self._parent, k)
            setattr(self._parent, k, v)

    def __exit__(self, *args):
        for k, v in self._old.items():
            setattr(self._parent, k, v)
        return False


def _dsop(create_type=None, wrap_missing_path=True):
    import functools

    def real_decorator(fn):
        @functools.wraps(fn)
        def newfn(self, key, *args, **kwargs):
            try:
                return fn(self, key, *args, **kwargs)
            except E.NotFoundError:
                if kwargs.get('create'):
                    try:
                        self.insert(key, create_type())
                    except E.KeyExistsError:
                        pass
                    return fn(self, key, *args, **kwargs)
                else:
                    raise
            except E.SubdocPathNotFoundError:
                if wrap_missing_path:
                    raise IndexError(args[0])

        return newfn

    return real_decorator


class Bucket(CoreBucket):

    def pipeline(self):
        """
        Returns a new :class:`Pipeline` context manager. When the
        context manager is active, operations performed will return
        ``None``, and will be sent on the network when the context
        leaves (in its ``__exit__`` method). To get the results of the
        pipelined operations, inspect the :attr:`Pipeline.results`
        property.

        Operational errors (i.e. negative replies from the server, or
        network errors) are delivered when the pipeline exits, but
        argument errors are thrown immediately.

        :return: a :class:`Pipeline` object
        :raise: :exc:`.PipelineError` if a pipeline is already created
        :raise: Other operation-specific errors.

        Scheduling multiple operations, without checking results::

          with cb.pipeline():
            cb.upsert("key1", "value1")
            cb.counter("counter")
            cb.upsert_multi({
              "new_key1" : "new_value_1",
              "new_key2" : "new_value_2"
            })

        Retrieve the results for several operations::

          pipeline = cb.pipeline()
          with pipeline:
            cb.upsert("foo", "bar")
            cb.replace("something", "value")

          for result in pipeline.results:
            print("Pipeline result: CAS {0}".format(result.cas))

        .. note::

          When in pipeline mode, you cannot execute view queries.
          Additionally, pipeline mode is not supported on async handles

        .. warning::

          Pipeline mode should not be used if you are using the same
          object concurrently from multiple threads. This only refers
          to the internal lock within the object itself. It is safe
          to use if you employ your own locking mechanism (for example
          a connection pool)

        .. versionadded:: 1.2.0

        """
        return Pipeline(self)

    # We have these wrappers so that IDEs can do param tooltips and the
    # like. we might move this directly into C some day

    def upsert(self, key, value, cas=0, ttl=0, format=None,
               persist_to=0, replicate_to=0):
        # type: (Union[str, bytes], Any, int, int, int, int, int, int) -> Result
        """Unconditionally store the object in Couchbase.

        :param key:
            The key to set the value with. By default, the key must be
            either a :class:`bytes` or :class:`str` object encodable as
            UTF-8. If a custom `transcoder` class is used (see
            :meth:`~__init__`), then the key object is passed directly
            to the transcoder, which may serialize it how it wishes.
        :type key: string or bytes

        :param value: The value to set for the key.
            This should be a native Python value which will be transparently
            serialized to JSON by the library. Do not pass already-serialized
            JSON as the value or it will be serialized again.

            If you are using a different `format` setting (see `format`
            parameter), and/or a custom transcoder then value for this
            argument may need to conform to different criteria.

        :param int cas: The _CAS_ value to use. If supplied, the value
            will only be stored if it already exists with the supplied
            CAS

        :param int ttl: If specified, the key will expire after this
            many seconds

        :param int format: If specified, indicates the `format` to use
            when encoding the value. If none is specified, it will use
            the `default_format` For more info see
            :attr:`~.default_format`

        :param int persist_to:
            Perform durability checking on this many nodes nodes for
            persistence to disk. See :meth:`endure` for more information

        :param int replicate_to: Perform durability checking on this
            many replicas for presence in memory. See :meth:`endure` for
            more information.

        :raise: :exc:`.ArgumentError` if an argument is supplied that is
            not applicable in this context. For example setting the CAS
            as a string.
        :raise: :exc`.CouchbaseNetworkError`
        :raise: :exc:`.KeyExistsError` if the key already exists on the
            server with a different CAS value.
        :raise: :exc:`.ValueFormatError` if the value cannot be
            serialized with chosen encoder, e.g. if you try to store a
            dictionary in plain mode.
        :return: :class:`~.Result`.

        Simple set::

            cb.upsert('key', 'value')

        Force JSON document format for value::

            cb.upsert('foo', {'bar': 'baz'}, format=couchbase_v2.FMT_JSON)

        Insert JSON from a string::

            JSONstr = '{"key1": "value1", "key2": 123}'
            JSONobj = json.loads(JSONstr)
            cb.upsert("documentID", JSONobj, format=couchbase_v2.FMT_JSON)

        Force UTF8 document format for value::

            cb.upsert('foo', "<xml></xml>", format=couchbase_v2.FMT_UTF8)

        Perform optimistic locking by specifying last known CAS version::

            cb.upsert('foo', 'bar', cas=8835713818674332672)

        Several sets at the same time (mutli-set)::

            cb.upsert_multi({'foo': 'bar', 'baz': 'value'})

        .. seealso:: :meth:`upsert_multi`
        """
        pass

    def upsert(self, key, value, *args, **kwargs):
        return _Base.upsert(self, key, value, *args, **kwargs)

    @overload
    def insert(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        """Store an object in Couchbase unless it already exists.

        Follows the same conventions as :meth:`upsert` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        Notably missing from this method is the `cas` parameter, this is
        because `insert` will only succeed if a key does not already
        exist on the server (and thus can have no CAS)

        :raise: :exc:`.KeyExistsError` if the key already exists

        .. seealso:: :meth:`upsert`, :meth:`insert_multi`
        """
        pass

    def insert(self, key, value, *args, **kwargs):
        return _Base.insert(self, key, value, *args, **kwargs)

    @overload
    def replace(self, key, value, cas=0, ttl=0, format=None,
                persist_to=0, replicate_to=0):
        """Store an object in Couchbase only if it already exists.

        Follows the same conventions as :meth:`upsert`, but the value is
        stored only if a previous value already exists.

        :raise: :exc:`.NotFoundError` if the key does not exist

        .. seealso:: :meth:`upsert`, :meth:`replace_multi`
        """
        pass

    def replace(self, key, value, *args, **kwargs):
        return _Base.replace(self, key, value, *args, **kwargs)

    def append(self,
               key,  # type: str
               value,  # type: couchbase_v2.JSON
               cas=0,  # type: long
               format=None,  # type: long
               persist_to=0,  # type: int
               replicate_to=0  # type: int
               ):
        # type: (...) -> couchbase_v2.result.Result
        """Append a string to an existing value in Couchbase.

        :param string value: The data to append to the existing value.

        Other parameters follow the same conventions as :meth:`upsert`.

        The `format` argument must be one of
        :const:`~couchbase_v2.FMT_UTF8` or :const:`~couchbase_v2.FMT_BYTES`.
        If not specified, it will be :const:`~.FMT_UTF8` (overriding the
        :attr:`default_format` attribute). This is because JSON or
        Pickle formats will be nonsensical when random data is appended
        to them. If you wish to modify a JSON or Pickle encoded object,
        you will need to retrieve it (via :meth:`get`), modify it, and
        then store it again (using :meth:`upsert`).

        Additionally, you must ensure the value (and flags) for the
        current value is compatible with the data to be appended. For an
        example, you may append a :const:`~.FMT_BYTES` value to an
        existing :const:`~couchbase_v2.FMT_JSON` value, but an error will
        be thrown when retrieving the value using :meth:`get` (you may
        still use the :attr:`data_passthrough` to overcome this).

        :raise: :exc:`.NotStoredError` if the key does not exist

        .. seealso:: :meth:`upsert`, :meth:`append_multi`
        """
        pass

    def append(self, key, value, *args, **kwargs):
        return _Base.append(self, key, value, *args, **kwargs)

    def prepend(self, key, value, cas=0, format=None,
                persist_to=0, replicate_to=0):
        # type: (...)->Result
        """Prepend a string to an existing value in Couchbase.

        .. seealso:: :meth:`append`, :meth:`prepend_multi`
        """

    def prepend(self, key, value, *args, **kwargs):
        return _Base.prepend(self, key, value, *args, **kwargs)

    def get(self,  # type: Bucket
            key,  # type: str
            ttl=0,  # type: int
            quiet=None,  # type: bool
            replica=False,  # type: bool
            no_format=False  # type: bool
            ):
        # type: (...)->couchbase_v2.result.Result
        """Obtain an object stored in Couchbase by given key.

        :param string key: The key to fetch. The type of key is the same
            as mentioned in :meth:`upsert`

        :param int ttl: If specified, indicates that the key's expiration
            time should be *modified* when retrieving the value.

        :param boolean quiet: causes `get` to return None instead of
            raising an exception when the key is not found. It defaults
            to the value set by :attr:`~quiet` on the instance. In
            `quiet` mode, the error may still be obtained by inspecting
            the :attr:`~.Result.rc` attribute of the :class:`.Result`
            object, or checking :attr:`.Result.success`.

            Note that the default value is `None`, which means to use
            the :attr:`quiet`. If it is a boolean (i.e. `True` or
            `False`) it will override the `couchbase_v2.bucket.Bucket`-level
            :attr:`quiet` attribute.

        :param bool replica: Whether to fetch this key from a replica
            rather than querying the master server. This is primarily
            useful when operations with the master fail (possibly due to
            a configuration change). It should normally be used in an
            exception handler like so

            Using the ``replica`` option::

                try:
                    res = c.get("key", quiet=True) # suppress not-found errors
                catch CouchbaseError:
                    res = c.get("key", replica=True, quiet=True)

        :param bool no_format: If set to ``True``, then the value will
            always be delivered in the :class:`~couchbase_v2.result.Result`
            object as being of :data:`~couchbase_v2.FMT_BYTES`. This is a
            item-local equivalent of using the :attr:`data_passthrough`
            option

        :raise: :exc:`.NotFoundError` if the key does not exist
        :raise: :exc:`.CouchbaseNetworkError`
        :raise: :exc:`.ValueFormatError` if the value cannot be
            deserialized with chosen decoder, e.g. if you try to
            retreive an object stored with an unrecognized format
        :return: A :class:`~.Result` object

        Simple get::

            value = cb.get('key').value

        Get multiple values::

            cb.get_multi(['foo', 'bar'])
            # { 'foo' : <Result(...)>, 'bar' : <Result(...)> }

        Inspect the flags::

            rv = cb.get("key")
            value, flags, cas = rv.value, rv.flags, rv.cas

        Update the expiration time::

            rv = cb.get("key", ttl=10)
            # Expires in ten seconds

        .. seealso:: :meth:`get_multi`
        """

        return _Base.get(self, key, ttl=ttl, quiet=quiet,
                         replica=replica, no_format=no_format)

    def touch(self, key, ttl=0):
        """Update a key's expiration time

        :param string key: The key whose expiration time should be
            modified
        :param int ttl: The new expiration time. If the expiration time
            is `0` then the key never expires (and any existing
            expiration is removed)
        :return: :class:`.OperationResult`

        Update the expiration time of a key ::

            cb.upsert("key", ttl=100)
            # expires in 100 seconds
            cb.touch("key", ttl=0)
            # key should never expire now

        :raise: The same things that :meth:`get` does

        .. seealso:: :meth:`get` - which can be used to get *and* update the
            expiration, :meth:`touch_multi`
        """
        return _Base.touch(self, key, ttl=ttl)

    def lock(self, key, ttl=0):
        """Lock and retrieve a key-value entry in Couchbase.

        :param key: A string which is the key to lock.

        :param ttl: a TTL for which the lock should be valid.
            While the lock is active, attempts to access the key (via
            other :meth:`lock`, :meth:`upsert` or other mutation calls)
            will fail with an :exc:`.KeyExistsError`. Note that the
            value for this option is limited by the maximum allowable
            lock time determined by the server (currently, this is 30
            seconds). If passed a higher value, the server will silently
            lower this to its maximum limit.


        This function otherwise functions similarly to :meth:`get`;
        specifically, it will return the value upon success. Note the
        :attr:`~.Result.cas` value from the :class:`.Result` object.
        This will be needed to :meth:`unlock` the key.

        Note the lock will also be implicitly released if modified by
        one of the :meth:`upsert` family of functions when the valid CAS
        is supplied

        :raise: :exc:`.TemporaryFailError` if the key is already locked.
        :raise: See :meth:`get` for possible exceptions

        Lock a key ::

            rv = cb.lock("locked_key", ttl=5)
            # This key is now locked for the next 5 seconds.
            # attempts to access this key will fail until the lock
            # is released.

            # do important stuff...

            cb.unlock("locked_key", rv.cas)

        Lock a key, implicitly unlocking with :meth:`upsert` with CAS ::

            rv = self.cb.lock("locked_key", ttl=5)
            new_value = rv.value.upper()
            cb.upsert("locked_key", new_value, rv.cas)


        Poll and Lock ::

            rv = None
            begin_time = time.time()
            while time.time() - begin_time < 15:
                try:
                    rv = cb.lock("key", ttl=10)
                    break
                except TemporaryFailError:
                    print("Key is currently locked.. waiting")
                    time.sleep(1)

            if not rv:
                raise Exception("Waited too long..")

            # Do stuff..

            cb.unlock("key", rv.cas)


        .. seealso:: :meth:`get`, :meth:`lock_multi`, :meth:`unlock`
        """
        return _Base.lock(self, key, ttl=ttl)

    def unlock(self, key, cas):
        """Unlock a Locked Key in Couchbase.

        This unlocks an item previously locked by :meth:`lock`

        :param key: The key to unlock
        :param cas: The cas returned from :meth:`lock`'s
            :class:`.Result` object.

        See :meth:`lock` for an example.

        :raise: :exc:`.TemporaryFailError` if the CAS supplied does not
            match the CAS on the server (possibly because it was
            unlocked by previous call).

        .. seealso:: :meth:`lock` :meth:`unlock_multi`
        """
        return _Base.unlock(self, key, cas=cas)

    def remove(self, key, cas=0, quiet=None, persist_to=0, replicate_to=0):
        """Remove the key-value entry for a given key in Couchbase.

        :param key: A string which is the key to remove. The format and
            type of the key follows the same conventions as in
            :meth:`upsert`
        :type key: string, dict, or tuple/list

        :param int cas: The CAS to use for the removal operation.
            If specified, the key will only be removed from the server
            if it has the same CAS as specified. This is useful to
            remove a key only if its value has not been changed from the
            version currently visible to the client. If the CAS on the
            server does not match the one specified, an exception is
            thrown.
        :param boolean quiet:
            Follows the same semantics as `quiet` in :meth:`get`
        :param int persist_to: If set, wait for the item to be removed
            from the storage of at least these many nodes
        :param int replicate_to: If set, wait for the item to be removed
            from the cache of at least these many nodes
            (excluding the master)
        :raise: :exc:`.NotFoundError` if the key does not exist.
        :raise: :exc:`.KeyExistsError` if a CAS was specified, but
            the CAS on the server had changed
        :return: A :class:`~.Result` object.

        Simple remove::

            ok = cb.remove("key").success

        Don't complain if key does not exist::

            ok = cb.remove("key", quiet=True)

        Only remove if CAS matches our version::

            rv = cb.get("key")
            cb.remove("key", cas=rv.cas)

        Remove multiple keys::

            oks = cb.remove_multi(["key1", "key2", "key3"])

        Remove multiple keys with CAS::

            oks = cb.remove({
                "key1" : cas1,
                "key2" : cas2,
                "key3" : cas3
            })

        .. seealso:: :meth:`remove_multi`, :meth:`endure`
            for more information on the ``persist_to`` and
            ``replicate_to`` options.
        """
        return _Base.remove(self, key, cas=cas, quiet=quiet,
                            persist_to=persist_to, replicate_to=replicate_to)

    def counter(self, key, delta=1, initial=None, ttl=0):
        """Increment or decrement the numeric value of an item.

        This method instructs the server to treat the item stored under
        the given key as a numeric counter.

        Counter operations require that the stored value
        exists as a string representation of a number (e.g. ``123``). If
        storing items using the :meth:`upsert` family of methods, and
        using the default :const:`couchbase_v2.FMT_JSON` then the value
        will conform to this constraint.

        :param string key: A key whose counter value is to be modified
        :param int delta: an amount by which the key should be modified.
            If the number is negative then this number will be
            *subtracted* from the current value.
        :param initial: The initial value for the key, if it does not
            exist. If the key does not exist, this value is used, and
            `delta` is ignored. If this parameter is `None` then no
            initial value is used
        :type initial: int or `None`
        :param int ttl: The lifetime for the key, after which it will
            expire
        :raise: :exc:`.NotFoundError` if the key does not exist on the
            bucket (and `initial` was `None`)
        :raise: :exc:`.DeltaBadvalError` if the key exists, but the
            existing value is not numeric
        :return: A :class:`.Result` object. The current value of the
            counter may be obtained by inspecting the return value's
            `value` attribute.

        Simple increment::

            rv = cb.counter("key")
            rv.value
            # 42

        Increment by 10::

            rv = cb.counter("key", delta=10)

        Decrement by 5::

            rv = cb.counter("key", delta=-5)

        Increment by 20, set initial value to 5 if it does not exist::

            rv = cb.counter("key", delta=20, initial=5)

        Increment three keys::

            kv = cb.counter_multi(["foo", "bar", "baz"])
            for key, result in kv.items():
                print "Key %s has value %d now" % (key, result.value)

        .. seealso:: :meth:`counter_multi`
        """
        return _Base.counter(self, key, delta=delta, initial=initial, ttl=ttl)

    def retrieve_in(self, key, *paths, **kwargs):
        """Atomically fetch one or more paths from a document.

        Convenience method for retrieval operations. This functions
        identically to :meth:`lookup_in`. As such, the following two
        forms are equivalent:

        .. code-block:: python

            import couchbase_v2.subdocument as SD
            rv = cb.lookup_in(key,
                              SD.get('email'),
                              SD.get('name'),
                              SD.get('friends.therock')

            email, name, friend = rv

        .. code-block:: python

            rv = cb.retrieve_in(key, 'email', 'name', 'friends.therock')
            email, name, friend = rv

        .. seealso:: :meth:`lookup_in`
        """
        import couchbase_core.subdocument as SD
        return self.lookup_in(key, *tuple(SD.get(x) for x in paths), **kwargs)

    def incr(self, key, amount=1, **kwargs):
        _depr('incr', 'counter')
        return self.counter(key, delta=amount, **kwargs)

    def incr_multi(self, keys, amount=1, **kwargs):
        _depr('incr_multi', 'counter_multi')
        return self.counter_multi(keys, delta=amount, **kwargs)

    def decr(self, key, amount=1, **kwargs):
        _depr('decr', 'counter')
        return self.counter(key, delta=-amount, **kwargs)

    def decr_multi(self, keys, amount=1, **kwargs):
        _depr('decr_multi', 'counter_multi')
        return self.counter_multi(keys, delta=-amount, **kwargs)

    def stats(self, keys=None, keystats=False):
        """Request server statistics.

        Fetches stats from each node in the cluster. Without a key
        specified the server will respond with a default set of
        statistical information. It returns the a `dict` with stats keys
        and node-value pairs as a value.

        :param keys: One or several stats to query
        :type keys: string or list of string
        :raise: :exc:`.CouchbaseNetworkError`
        :return: `dict` where keys are stat keys and values are
            host-value pairs

        Find out how many items are in the bucket::

            total = 0
            for key, value in cb.stats()['total_items'].items():
                total += value

        Get memory stats (works on couchbase buckets)::

            cb.stats('memory')
            # {'mem_used': {...}, ...}
        """
        if keys and not isinstance(keys, (tuple, list)):
            keys = (keys,)
        return self._stats(keys, keystats=keystats)

    def ping(self):
        """Ping cluster for latency/status information per-service

        Pings each node in the cluster, and
        returns a `dict` with 'type' keys (e.g 'n1ql', 'kv')
        and node service summary lists as a value.


        :raise: :exc:`.CouchbaseNetworkError`
        :return: `dict` where keys are stat keys and values are
            host-value pairs

        Ping cluster (works on couchbase buckets)::

            cb.ping()
            # {'services': {...}, ...}
        """
        resultdict = self._ping()
        return resultdict['services_struct']

    def diagnostics(self):
        """Request diagnostics report about network connections

        Generates diagnostics for each node in the cluster.
        It returns a `dict` with details


        :raise: :exc:`.CouchbaseNetworkError`
        :return: `dict` where keys are stat keys and values are
            host-value pairs

        Get health info (works on couchbase buckets)::

            cb.diagnostics()
            # {
                  'config':
                  {
                     'id': node ID,
                     'last_activity_us': time since last activity in nanoseconds
                     'local': local server and port,
                     'remote': remote server and port,
                     'status': connection status
                  }
                  'id': client ID,
                  'sdk': sdk version,
                  'version': diagnostics API version
              }
        """
        return json.loads(self._diagnostics()['health_json'])

    def observe(self, key, master_only=False):
        """Return storage information for a key.

        It returns a :class:`.ValueResult` object with the ``value``
        field set to a list of :class:`~.ObserveInfo` objects. Each
        element in the list responds to the storage status for the key
        on the given node. The length of the list (and thus the number
        of :class:`~.ObserveInfo` objects) are equal to the number of
        online replicas plus the master for the given key.

        :param string key: The key to inspect
        :param bool master_only: Whether to only retrieve information
            from the master node.

        .. seealso:: :ref:`observe_info`
        """
        return _Base.observe(self, key, master_only=master_only)

    def endure(self, key, persist_to=-1, replicate_to=-1, cas=0,
               check_removed=False, timeout=5.0, interval=0.010):
        """Wait until a key has been distributed to one or more nodes

        By default, when items are stored to Couchbase, the operation is
        considered successful if the vBucket master (i.e. the "primary"
        node) for the key has successfully stored the item in its
        memory.

        In most situations, this is sufficient to assume that the item
        has successfully been stored. However the possibility remains
        that the "master" server will go offline as soon as it sends
        back the successful response and the data is lost.

        The ``endure`` function allows you to provide stricter criteria
        for success. The criteria may be expressed in terms of number of
        nodes for which the item must exist in that node's RAM and/or on
        that node's disk. Ensuring that an item exists in more than one
        place is a safer way to guarantee against possible data loss.

        We call these requirements `Durability Constraints`, and thus
        the method is called `endure`.

        :param string key: The key to endure.
        :param int persist_to: The minimum number of nodes which must
            contain this item on their disk before this function
            returns. Ensure that you do not specify too many nodes;
            otherwise this function will fail. Use the
            :attr:`server_nodes` to determine how many nodes exist in
            the cluster.

            The maximum number of nodes an item can reside on is
            currently fixed to 4 (i.e. the "master" node, and up to
            three "replica" nodes). This limitation is current as of
            Couchbase Server version 2.1.0.

            If this parameter is set to a negative value, the maximum
            number of possible nodes the key can reside on will be used.

        :param int replicate_to: The minimum number of replicas which
            must contain this item in their memory for this method to
            succeed. As with ``persist_to``, you may specify a negative
            value in which case the requirement will be set to the
            maximum number possible.

        :param float timeout: A timeout value in seconds before this
            function fails with an exception. Typically it should take
            no longer than several milliseconds on a functioning cluster
            for durability requirements to be satisfied (unless
            something has gone wrong).

        :param float interval: The polling interval in seconds to use
            for checking the key status on the respective nodes.
            Internally, ``endure`` is implemented by polling each server
            individually to see if the key exists on that server's disk
            and memory. Once the status request is sent to all servers,
            the client will check if their replies are satisfactory; if
            they are then this function succeeds, otherwise the client
            will wait a short amount of time and try again. This
            parameter sets this "wait time".

        :param bool check_removed: This flag inverts the check. Instead
            of checking that a given key *exists* on the nodes, this
            changes the behavior to check that the key is *removed* from
            the nodes.

        :param long cas: The CAS value to check against. It is possible
            for an item to exist on a node but have a CAS value from a
            prior operation. Passing the CAS ensures that only replies
            from servers with a CAS matching this parameter are accepted.

        :return: A :class:`~.OperationResult`
        :raise: see :meth:`upsert` and :meth:`get` for possible errors

        .. seealso:: :meth:`upsert`, :meth:`endure_multi`
        """
        # We really just wrap 'endure_multi'
        kv = {key: cas}
        rvs = self.endure_multi(keys=kv, persist_to=persist_to,
                                replicate_to=replicate_to,
                                check_removed=check_removed, timeout=timeout,
                                interval=interval)
        return rvs[key]

    def durability(self, persist_to=-1, replicate_to=-1, timeout=0.0):
        """Returns a context manager which will apply the given
        persistence/replication settings to all mutation operations when
        active

        :param int persist_to:
        :param int replicate_to:

        See :meth:`endure` for the meaning of these two values

        Thus, something like::

          with cb.durability(persist_to=3):
            cb.upsert("foo", "foo_value")
            cb.upsert("bar", "bar_value")
            cb.upsert("baz", "baz_value")

        is equivalent to::

            cb.upsert("foo", "foo_value", persist_to=3)
            cb.upsert("bar", "bar_value", persist_to=3)
            cb.upsert("baz", "baz_value", persist_to=3)


        .. versionadded:: 1.2.0

        .. seealso:: :meth:`endure`
        """
        return DurabilityContext(self, persist_to, replicate_to, timeout)

    def upsert_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        """
        Write multiple items to the cluster. Multi version of :meth:`upsert`

        :param dict keys: A dictionary of keys to set. The keys are the
            keys as they should be on the server, and the values are the
            values for the keys to be stored.

            `keys` may also be a :class:`~.ItemCollection`. If using a
            dictionary variant for item collections, an additional
            `ignore_cas` parameter may be supplied with a boolean value.
            If not specified, the operation will fail if the CAS value
            on the server does not match the one specified in the
            `Item`'s `cas` field.
        :param int ttl: If specified, sets the expiration value
            for all keys
        :param int format: If specified, this is the conversion format
            which will be used for _all_ the keys.
        :param int persist_to: Durability constraint for persistence.
            Note that it is more efficient to use :meth:`endure_multi`
            on the returned :class:`~couchbase_v2.result.MultiResult` than
            using these parameters for a high volume of keys. Using
            these parameters however does save on latency as the
            constraint checking for each item is performed as soon as it
            is successfully stored.
        :param int replicate_to: Durability constraints for replication.
            See notes on the `persist_to` parameter for usage.
        :return: A :class:`~.MultiResult` object, which is a
            `dict`-like object

        The multi methods are more than just a convenience, they also
        save on network performance by batch-scheduling operations,
        reducing latencies. This is especially noticeable on smaller
        value sizes.

        .. seealso:: :meth:`upsert`
        """
        return _Base.upsert_multi(self, keys, ttl=ttl, format=format,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to)

    def insert_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        """Add multiple keys. Multi variant of :meth:`insert`

        .. seealso:: :meth:`insert`, :meth:`upsert_multi`, :meth:`upsert`
        """
        return _Base.insert_multi(self, keys, ttl=ttl, format=format,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to)

    def replace_multi(self, keys, ttl=0, format=None,
                      persist_to=0, replicate_to=0):
        """Replace multiple keys. Multi variant of :meth:`replace`

        .. seealso:: :meth:`replace`, :meth:`upsert_multi`, :meth:`upsert`
        """
        return _Base.replace_multi(self, keys, ttl=ttl, format=format,
                                   persist_to=persist_to,
                                   replicate_to=replicate_to)

    def append_multi(self, keys, format=None, persist_to=0, replicate_to=0):
        """Append to multiple keys. Multi variant of :meth:`append`.

        .. warning::

            If using the `Item` interface, use the :meth:`append_items`
            and :meth:`prepend_items` instead, as those will
            automatically update the :attr:`.Item.value`
            property upon successful completion.

        .. seealso:: :meth:`append`, :meth:`upsert_multi`, :meth:`upsert`
        """
        return _Base.append_multi(self, keys, format=format,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to)

    def prepend_multi(self, keys, format=None, persist_to=0, replicate_to=0):
        """Prepend to multiple keys. Multi variant of :meth:`prepend`

        .. seealso:: :meth:`prepend`, :meth:`upsert_multi`, :meth:`upsert`
        """
        return _Base.prepend_multi(self, keys, format=format,
                                   persist_to=persist_to,
                                   replicate_to=replicate_to)

    def get_multi(self, keys, ttl=0, quiet=None, replica=False, no_format=False):
        """Get multiple keys. Multi variant of :meth:`get`

        :param keys: keys the keys to fetch
        :type keys: :ref:`iterable<argtypes>`
        :param int ttl: Set the expiration for all keys when retrieving
        :param boolean replica:
            Whether the results should be obtained from a replica
            instead of the master. See :meth:`get` for more information
            about this parameter.
        :return: A :class:`~.MultiResult` object. This is a dict-like
            object  and contains the keys (passed as) `keys` as the
            dictionary keys, and :class:`~.Result` objects as values
        """
        return _Base.get_multi(self, keys, ttl=ttl, quiet=quiet,
                               replica=replica, no_format=no_format)

    def touch_multi(self, keys, ttl=0):
        """Touch multiple keys. Multi variant of :meth:`touch`

        :param keys: the keys to touch
        :type keys: :ref:`iterable<argtypes>`.
            ``keys`` can also be a dictionary with values being
            integers, in which case the value for each key will be used
            as the TTL instead of the global one (i.e. the one passed to
            this function)
        :param int ttl: The new expiration time
        :return: A :class:`~.MultiResult` object

        Update three keys to expire in 10 seconds ::

            cb.touch_multi(("key1", "key2", "key3"), ttl=10)

        Update three keys with different expiration times ::

            cb.touch_multi({"foo" : 1, "bar" : 5, "baz" : 10})

        .. seealso:: :meth:`touch`
        """
        return _Base.touch_multi(self, keys, ttl=ttl)

    def lock_multi(self, keys, ttl=0):
        """Lock multiple keys. Multi variant of :meth:`lock`

        :param keys: the keys to lock
        :type keys: :ref:`iterable<argtypes>`
        :param int ttl: The lock timeout for all keys

        :return: a :class:`~.MultiResult` object

        .. seealso:: :meth:`lock`
        """
        return _Base.lock_multi(self, keys, ttl=ttl)

    def unlock_multi(self, keys):
        """Unlock multiple keys. Multi variant of :meth:`unlock`

        :param dict keys: the keys to unlock
        :return: a :class:`~couchbase_v2.result.MultiResult` object

        The value of the ``keys`` argument should be either the CAS, or
        a previously returned :class:`Result` object from a :meth:`lock`
        call. Effectively, this means you may pass a
        :class:`~.MultiResult` as the ``keys`` argument.

        Thus, you can do something like ::

            keys = (....)
            rvs = cb.lock_multi(keys, ttl=5)
            # do something with rvs
            cb.unlock_multi(rvs)

        .. seealso:: :meth:`unlock`
        """
        return _Base.unlock_multi(self, keys)

    def observe_multi(self, keys, master_only=False):
        """Multi-variant of :meth:`observe`"""
        return _Base.observe_multi(self, keys, master_only=master_only)

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1,
                     timeout=5.0, interval=0.010, check_removed=False):
        """Check durability requirements for multiple keys

        :param keys: The keys to check

        The type of keys may be one of the following:
            * Sequence of keys
            * A :class:`~couchbase_v2.result.MultiResult` object
            * A ``dict`` with CAS values as the dictionary value
            * A sequence of :class:`~couchbase_v2.result.Result` objects

        :return: A :class:`~.MultiResult` object
            of :class:`~.OperationResult` items.

        .. seealso:: :meth:`endure`
        """
        if not _LCB.PYCBC_ENDURE:
            raise NotImplementedInV3("Standalone endure")
        return _Base.endure_multi(self, keys, persist_to=persist_to,
                                  replicate_to=replicate_to,
                                  timeout=timeout, interval=interval,
                                  check_removed=check_removed)

    def remove_multi(self, kvs, quiet=None):
        """Remove multiple items from the cluster

        :param kvs: Iterable of keys to delete from the cluster. If you wish
            to specify a CAS for each item, then you may pass a dictionary
            of keys mapping to cas, like `remove_multi({k1:cas1, k2:cas2}`)
        :param quiet: Whether an exception should be raised if one or more
            items were not found
        :return: A :class:`~.MultiResult` containing :class:`~.OperationResult`
            values.
        """
        return _Base.remove_multi(self, kvs, quiet=quiet)

    def counter_multi(self, kvs, initial=None, delta=1, ttl=0):
        """Perform counter operations on multiple items

        :param kvs: Keys to operate on. See below for more options
        :param initial: Initial value to use for all keys.
        :param delta: Delta value for all keys.
        :param ttl: Expiration value to use for all keys

        :return: A :class:`~.MultiResult` containing :class:`~.ValueResult`
            values


        The `kvs` can be a:

        - Iterable of keys
            .. code-block:: python

                cb.counter_multi((k1, k2))

        - A dictionary mapping a key to its delta
            .. code-block:: python

                cb.counter_multi({
                    k1: 42,
                    k2: 99
                })

        - A dictionary mapping a key to its additional options
            .. code-block:: python

                cb.counter_multi({
                    k1: {'delta': 42, 'initial': 9, 'ttl': 300},
                    k2: {'delta': 99, 'initial': 4, 'ttl': 700}
                })


        When using a dictionary, you can override settings for each key on
        a per-key basis (for example, the initial value). Global settings
        (global here means something passed as a parameter to the method)
        will take effect for those values which do not have a given option
        specified.
        """
        return _Base.counter_multi(self, kvs, initial=initial, delta=delta,
                                   ttl=ttl)

    def bucket_manager(self):
        """
        Returns a :class:`~.BucketManager` object which may be used to
        perform management operations on the current bucket. These
        operations may create/modify design documents and flush the
        bucket
        """
        return BucketManager(self)

    n1ql_query = CoreBucket.query
    query = CoreBucket.view_query

    def analytics_query(self, query, host, *args, **kwargs):
        """
        Execute an Analytics query.

        This method is mainly a wrapper around the :class:`~.AnalyticsQuery`
        and :class:`~.AnalyticsRequest` objects, which contain the inputs
        and outputs of the query.

        Using an explicit :class:`~.AnalyticsQuery`::

            query = AnalyticsQuery(
                "SELECT VALUE bw FROM breweries bw WHERE bw.name = ?", "Kona Brewing")
            for row in cb.analytics_query(query, "127.0.0.1"):
                print('Entry: {0}'.format(row))

        Using an implicit :class:`~.AnalyticsQuery`::

            for row in cb.analytics_query(
                "SELECT VALUE bw FROM breweries bw WHERE bw.name = ?", "127.0.0.1", "Kona Brewing"):
                print('Entry: {0}'.format(row))

        :param query: The query to execute. This may either be a
            :class:`.AnalyticsQuery` object, or a string (which will be
            implicitly converted to one).
        :param host: The host to send the request to.
        :param args: Positional arguments for :class:`.AnalyticsQuery`.
        :param kwargs: Named arguments for :class:`.AnalyticsQuery`.
        :return: An iterator which yields rows. Each row is a dictionary
            representing a single result
        """
        if not isinstance(query, AnalyticsQuery):
            query = AnalyticsQuery(query, *args, **kwargs)
        else:
            query.update(*args, **kwargs)

        return couchbase_v2.analytics.gen_request(query, host, self)


    _analytics_query = analytics_query

    def search(self, index, query, **kwargs):
        """
        Perform full-text searches

        .. versionadded:: 2.0.9

        .. warning::

            The full-text search API is experimental and subject to change

        :param str index: Name of the index to query
        :param couchbase_v2.fulltext.SearchQuery query: Query to issue
        :param couchbase_v2.fulltext.Params params: Additional query options
        :return: An iterator over query hits

        .. note:: You can avoid instantiating an explicit `Params` object
            and instead pass the parameters directly to the `search` method.

        .. code-block:: python

            it = cb.search('name', ft.MatchQuery('nosql'), limit=10)
            for hit in it:
                print(hit)

        """
        itercls = kwargs.pop('itercls', _FTS.SearchRequest)
        iterargs = itercls.mk_kwargs(kwargs)
        params = kwargs.pop('params', _FTS.Params(**kwargs))
        body = _FTS.make_search_body(index, query, params)
        return itercls(body, self, **iterargs)

    # "items" interface
    def append_items(self, items, **kwargs):
        """
        Method to append data to multiple :class:`~.Item` objects.

        This method differs from the normal :meth:`append_multi` in that
        each `Item`'s `value` field is updated with the appended data
        upon successful completion of the operation.


        :param items: The item dictionary. The value for each key should
            contain a ``fragment`` field containing the object to append
            to the value on the server.
        :type items: :class:`~couchbase_v2.items.ItemOptionDict`.

        The rest of the options are passed verbatim to
        :meth:`append_multi`

        .. seealso:: :meth:`append_multi`, :meth:`append`
        """
        rv = self.append_multi(items, **kwargs)
        # Assume this is an 'ItemOptionDict'
        for k, v in items.dict.items():
            if k.success:
                k.value += v['fragment']

        return rv

    def prepend_items(self, items, **kwargs):
        """Method to prepend data to multiple :class:`~.Item` objects.
        .. seealso:: :meth:`append_items`
        """
        rv = self.prepend_multi(items, **kwargs)
        for k, v in items.dict.items():
            if k.success:
                k.value = v['fragment'] + k.value

        return rv

    _OLDOPS = { 'set': 'upsert', 'add': 'insert', 'delete': 'remove'}
    for o, n in _OLDOPS.items():
        for variant in ('', '_multi'):
            oldname = o + variant
            newname = n + variant

            try:
                dst = locals()[n + variant]
            except KeyError:
                dst = getattr(_Base, n + variant)

            def mkmeth(oldname, newname, _dst):
                def _tmpmeth(self, *args, **kwargs):
                    _depr(oldname, newname)
                    return _dst(self, *args, **kwargs)
                return _tmpmeth

            locals().update({oldname: mkmeth(oldname, newname, dst)})

    """
    Lists the names of all the memcached operations. This is useful
    for classes which want to wrap all the methods
    """
    _MEMCACHED_OPERATIONS = ('upsert', 'get', 'insert', 'append', 'prepend',
                             'replace', 'remove', 'counter', 'touch',
                             'lock', 'unlock', 'endure',
                             'observe', 'rget', 'stats',
                             'set', 'add', 'delete', 'lookup_in', 'mutate_in')

    _MEMCACHED_NOMULTI = ('stats', 'lookup_in', 'mutate_in')

    @classmethod
    def _gen_memd_wrappers(cls, factory):
        """Generates wrappers for all the memcached operations.
        :param factory: A function to be called to return the wrapped
            method. It will be called with two arguments; the first is
            the unbound method being wrapped, and the second is the name
            of such a method.

          The factory shall return a new unbound method

        :return: A dictionary of names mapping the API calls to the
            wrapped functions
        """
        d = {}
        for n in cls._MEMCACHED_OPERATIONS:
            for variant in (n, n + "_multi"):
                try:
                    d[variant] = factory(getattr(cls, variant), variant)
                except AttributeError:
                    if n in cls._MEMCACHED_NOMULTI:
                        continue
                    raise
        return d

    def design_get(self, *args, **kwargs):
        _depr('design_get', 'bucket_manager().design_get')
        return self.bucket_manager().design_get(*args, **kwargs)

    def design_create(self, *args, **kwargs):
        _depr('design_create', 'bucket_manager().design_create')
        return self.bucket_manager().design_create(*args, **kwargs)

    def design_publish(self, *args, **kwargs):
        _depr('design_publish', 'bucket_manager().design_publish')
        return self.bucket_manager().design_publish(*args, **kwargs)

    def design_delete(self, *args, **kwargs):
        _depr('design_delete', 'bucket_manager().design_delete')
        return self.bucket_manager().design_delete(*args, **kwargs)

    def add_bucket_creds(self, bucket, password):
        if not bucket or not password:
            raise ValueError('Bucket and password must be nonempty')
        return _Base._add_creds(self, bucket, password)

    @_dsop(create_type=dict)
    def map_add(self, key, mapkey, value, create=False, **kwargs):
        """
        Set a value for a key in a map.

        .. warning::

            The functionality of the various `map_*`, `list_*`, `queue_*`
            and `set_*` functions are considered experimental and are included
            in the library to demonstrate new functionality.
            They may change in the future or be removed entirely!

            These functions are all wrappers around the :meth:`mutate_in` or
            :meth:`lookup_in` methods.

        :param key: The document ID of the map
        :param mapkey: The key in the map to set
        :param value: The value to use (anything serializable to JSON)
        :param create: Whether the map should be created if it does not exist
        :param kwargs: Additional arguments passed to :meth:`mutate_in`
        :return: A :class:`~.OperationResult`
        :raise: :cb_exc:`NotFoundError` if the document does not exist.
            and `create` was not specified

        .. Initialize a map and add a value

            cb.upsert('a_map', {})
            cb.map_add('a_map', 'some_key', 'some_value')
            cb.map_get('a_map', 'some_key').value  # => 'some_value'
            cb.get('a_map').value  # => {'some_key': 'some_value'}

        """
        op = SD.upsert(mapkey, value)
        sdres = self.mutate_in(key, op, **kwargs)
        return self._wrap_dsop(sdres)

    @_dsop()
    def map_get(self, key, mapkey):
        """
        Retrieve a value from a map.

        :param str key: The document ID
        :param str mapkey: Key within the map to retrieve
        :return: :class:`~.ValueResult`
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`NotFoundError` if the document does not exist.

        .. seealso:: :meth:`map_add` for an example
        """
        op = SD.get(mapkey)
        sdres = self.lookup_in(key, op)
        return self._wrap_dsop(sdres, True)

    @_dsop()
    def map_remove(self, key, mapkey, **kwargs):
        """
        Remove an item from a map.

        :param str key: The document ID
        :param str mapkey: The key in the map
        :param kwargs: See :meth:`mutate_in` for options
        :raise: :exc:`IndexError` if the mapkey does not exist
        :raise: :cb_exc:`NotFoundError` if the document does not exist.

        .. Remove a map key-value pair:

            cb.map_remove('a_map', 'some_key')

        .. seealso:: :meth:`map_add`
        """
        op = SD.remove(mapkey)
        sdres = self.mutate_in(key, op, **kwargs)
        return self._wrap_dsop(sdres)

    @_dsop()
    def map_size(self, key):
        """
        Get the number of items in the map.

        :param str key: The document ID of the map
        :return int: The number of items in the map
        :raise: :cb_exc:`NotFoundError` if the document does not exist.

        .. seealso:: :meth:`map_add`
        """
        # TODO: This should use get_count, but we need to check for compat
        # with server version (i.e. >= 4.6) first; otherwise it just
        # disconnects.

        rv = self.get(key)
        return len(rv.value)

    @_dsop(create_type=list)
    def list_append(self, key, value, create=False, **kwargs):
        """
        Add an item to the end of a list.

        :param str key: The document ID of the list
        :param value: The value to append
        :param create: Whether the list should be created if it does not
               exist. Note that this option only works on servers >= 4.6
        :param kwargs: Additional arguments to :meth:`mutate_in`
        :return: :class:`~.OperationResult`.
        :raise: :cb_exc:`NotFoundError` if the document does not exist.
            and `create` was not specified.

        example::

            cb.list_append('a_list', 'hello')
            cb.list_append('a_list', 'world')

        .. seealso:: :meth:`map_add`
        """
        op = SD.array_append('', value)
        sdres = self.mutate_in(key, op, **kwargs)
        return self._wrap_dsop(sdres)

    @_dsop(create_type=list)
    def list_prepend(self, key, value, create=False, **kwargs):
        """
        Add an item to the beginning of a list.

        :param str key: Document ID
        :param value: Value to prepend
        :param bool create:
            Whether the list should be created if it does not exist
        :param kwargs: Additional arguments to :meth:`mutate_in`.
        :return: :class:`OperationResult`.
        :raise: :cb_exc:`NotFoundError` if the document does not exist.
            and `create` was not specified.

        This function is identical to :meth:`list_append`, except for prepending
        rather than appending the item

        .. seealso:: :meth:`list_append`, :meth:`map_add`
        """
        op = SD.array_prepend('', value)
        sdres = self.mutate_in(key, op, **kwargs)
        return self._wrap_dsop(sdres)

    @_dsop()
    def list_set(self, key, index, value, **kwargs):
        """
        Sets an item within a list at a given position.

        :param key: The key of the document
        :param index: The position to replace
        :param value: The value to be inserted
        :param kwargs: Additional arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :cb_exc:`NotFoundError` if the list does not exist
        :raise: :exc:`IndexError` if the index is out of bounds

        example::

            cb.upsert('a_list', ['hello', 'world'])
            cb.list_set('a_list', 1, 'good')
            cb.get('a_list').value # => ['hello', 'good']

        .. seealso:: :meth:`map_add`, :meth:`list_append`
        """
        op = SD.replace('[{0}]'.format(index), value)
        sdres = self.mutate_in(key, op, **kwargs)
        return self._wrap_dsop(sdres)

    @_dsop(create_type=list)
    def set_add(self, key, value, create=False, **kwargs):
        """
        Add an item to a set if the item does not yet exist.

        :param key: The document ID
        :param value: Value to add
        :param create: Create the set if it does not exist
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: A :class:`~.OperationResult` if the item was added,
        :raise: :cb_exc:`NotFoundError` if the document does not exist
            and `create` was not specified.

        .. seealso:: :meth:`map_add`
        """
        op = SD.array_addunique('', value)
        try:
            sdres = self.mutate_in(key, op, **kwargs)
            return self._wrap_dsop(sdres)
        except E.SubdocPathExistsError:
            pass

    @_dsop()
    def set_remove(self, key, value, **kwargs):
        """
        Remove an item from a set.

        :param key: The docuent ID
        :param value: Value to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: A :class:`OperationResult` if the item was removed, false
                 otherwise
        :raise: :cb_exc:`NotFoundError` if the set does not exist.

        .. seealso:: :meth:`set_add`, :meth:`map_add`
        """
        while True:
            rv = self.get(key)
            try:
                ix = rv.value.index(value)
                kwargs['cas'] = rv.cas
                return self.list_remove(key, ix, **kwargs)
            except E.KeyExistsError:
                pass
            except ValueError:
                return

    def set_size(self, key):
        """
        Get the length of a set.

        :param key: The document ID of the set
        :return: The length of the set
        :raise: :cb_exc:`NotFoundError` if the set does not exist.

        """
        return self.list_size(key)

    def set_contains(self, key, value):
        """
        Determine if an item exists in a set
        :param key: The document ID of the set
        :param value: The value to check for
        :return: True if `value` exists in the set
        :raise: :cb_exc:`NotFoundError` if the document does not exist
        """
        rv = self.get(key)
        return value in rv.value

    @_dsop()
    def list_get(self, key, index):
        """
        Get a specific element within a list.

        :param key: The document ID
        :param index: The index to retrieve
        :return: :class:`ValueResult` for the element
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`NotFoundError` if the list does not exist
        """
        return self.map_get(key, '[{0}]'.format(index))

    @_dsop()
    def list_remove(self, key, index, **kwargs):
        """
        Remove the element at a specific index from a list.

        :param key: The document ID of the list
        :param index: The index to remove
        :param kwargs: Arguments to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :exc:`IndexError` if the index does not exist
        :raise: :cb_exc:`NotFoundError` if the list does not exist
        """
        return self.map_remove(key, '[{0}]'.format(index), **kwargs)

    @_dsop()
    def list_size(self, key):
        """
        Retrieve the number of elements in the list.

        :param key: The document ID of the list
        :return: The number of elements within the list
        :raise: :cb_exc:`NotFoundError` if the list does not exist
        """
        return self.map_size(key)

    @_dsop(create_type=list)
    def queue_push(self, key, value, create=False, **kwargs):
        """
        Add an item to the end of a queue.

        :param key: The document ID of the queue
        :param value: The item to add to the queue
        :param create: Whether the queue should be created if it does not exist
        :param kwargs: Arguments to pass to :meth:`mutate_in`
        :return: :class:`OperationResult`
        :raise: :cb_exc:`NotFoundError` if the queue does not exist and
            `create` was not specified.

        example::

            # Ensure it's removed first

            cb.remove('a_queue')
            cb.queue_push('a_queue', 'job9999', create=True)
            cb.queue_pop('a_queue').value  # => job9999
        """
        return self.list_prepend(key, value, **kwargs)

    @_dsop()
    def queue_pop(self, key, **kwargs):
        """
        Remove and return the first item queue.

        :param key: The document ID
        :param kwargs: Arguments passed to :meth:`mutate_in`
        :return: A :class:`ValueResult`
        :raise: :cb_exc:`QueueEmpty` if there are no items in the queue.
        :raise: :cb_exc:`NotFoundError` if the queue does not exist.
        """
        while True:
            try:
                itm = self.list_get(key, -1)
            except IndexError:
                raise E.QueueEmpty

            kwargs['cas'] = itm.cas
            try:
                self.list_remove(key, -1, **kwargs)
                return itm
            except E.KeyExistsError:
                pass
            except IndexError:
                raise E.QueueEmpty

    @_dsop()
    def queue_size(self, key):
        """
        Get the length of the queue.

        :param key: The document ID of the queue
        :return: The length of the queue
        :raise: :cb_exc:`NotFoundError` if the queue does not exist.
        """
        return self.list_size(key)


    def get_attribute(self, key, attrname):
        pass

    def set_attribute(self, key, attrname):
        pass

    def register_crypto_provider(self, name, provider):
        """
        Registers the crypto provider used to encrypt and decrypt document fields.
        :param name: The name of the provider.
        :param provider: The provider implementation. // reference LCB type?
        """
        _Base.register_crypto_provider(self, name, provider)

    def unregister_crypto_provider(self, name):
        """
        Unregisters the crypto provider used to encrypt and decrypt document fields.
        :param name: The name of the provider.
        """
        _Base.unregister_crypto_provider(self, name)

    def encrypt_fields(self, document, fieldspec, prefix):
        """
        Encrypt a document using the registered encryption providers.
        :param document: The document body.
        :param fieldspec: A list of field specifications, each of which is
        a dictionary as follows:
            {
                'alg' : registered algorithm name,
                'kid' : key id to use to encrypt with,
                'name' : field name
            }
        :param prefix: Prefix for encrypted field names. Default is None.
        :return: Encrypted document.
        """
        json_encoded = json.dumps(document)
        encrypted_string = _Base.encrypt_fields(self, json_encoded, fieldspec, prefix)
        if not encrypted_string:
            raise couchbase_core.exceptions.CouchbaseError("Encryption failed")
        return json.loads(encrypted_string)

    def decrypt_fields_real(self, document, *args):
        json_decoded = json.dumps(document)
        decrypted_string = _Base.decrypt_fields(self, json_decoded, *args)
        if not decrypted_string:
            raise couchbase_core.exceptions.CouchbaseError("Decryption failed")
        return json.loads(decrypted_string)

    if _LCB.PYCBC_CRYPTO_VERSION<1:
        def decrypt_fields(self, document, prefix):
            """
            Decrypts a document using the registered encryption providers.
            :param document: The document body.
            :param prefix: Prefix for encrypted field names. Default is None.
            :return:
            """
            return self.decrypt_fields_real(document, prefix)
    else:
        def decrypt_fields(self, document, fieldspec, prefix):
            """
            Decrypts a document using the registered encryption providers.
            :param document: The document body.
            :param fieldspec: A list of field specifications, each of which is
            a dictionary as follows:
                {
                    'alg' : registered algorithm name,
                    'name' : field name
                }
            :param prefix: Prefix for encrypted field names. Default is None.
            :return:
            """
            return self.decrypt_fields_real(document, fieldspec, prefix)

class HLBucket(Bucket):
    pass