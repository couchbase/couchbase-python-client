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
import json
import time
from collections import deque


import couchbase._bootstrap
import couchbase._libcouchbase as _LCB
from couchbase._libcouchbase import Connection as _Base
from couchbase.iops.select import SelectIOPS

from couchbase.exceptions import *
from couchbase.user_constants import *
from couchbase.result import *

import couchbase.exceptions as exceptions
from couchbase.views.params import make_dvpath, make_options_string
from couchbase.views.iterator import View
from couchbase._pyport import basestring

class Pipeline(object):
    def __init__(self, parent):
        """

        .. versionadded:: 1.2.0

        Creates a new pipeline context. See :meth:`~Connection.pipeline`
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
        Contains a list of results for each pipelined operation executed within
        the context. The list remains until this context is reused.

        The elements in the list are either :class:`~couchbase.result.Result`
        objects (for single operations) or
        :class:`~couchbase.result.MultiResult` objects (for multi operations)
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

class Connection(_Base):

    def _gen_host_string(self, host, port):
        if not isinstance(host, (tuple, list)):
            return "{0}:{1}".format(host, port)

        hosts_tmp = []
        for curhost in host:
            cur_hname = None
            cur_hport = None
            if isinstance(curhost, (list, tuple)):
                cur_hname, cur_hport = curhost
            else:
                cur_hname = curhost
                cur_hport = port

            hosts_tmp.append("{0}:{1}".format(cur_hname, cur_hport))

        return ";".join(hosts_tmp)

    def __init__(self, **kwargs):
        """Connection to a bucket.

        Normally it's initialized through :meth:`couchbase.Couchbase.connect`

        See :meth:`couchbase.Couchbase.connect` for constructor options
        """
        bucket = kwargs.get('bucket', None)
        host = kwargs.get('host', 'localhost')
        username = kwargs.get('username', None)
        password = kwargs.get('password', None)

        # We don't pass this to the actual constructor
        port = kwargs.pop('port', 8091)
        _no_connect_exceptions = kwargs.pop('_no_connect_exceptions', False)
        _gevent_support = kwargs.pop('experimental_gevent_support', False)
        _cntlopts = kwargs.pop('_cntl', {})

        if not bucket:
            raise exceptions.ArgumentError("A bucket name must be given")

        kwargs['host'] = self._gen_host_string(host, port)
        kwargs['bucket'] = bucket

        if password and not username:
            kwargs['username'] = bucket

        # Internal parameters
        kwargs['_errors'] = deque(maxlen=1000)

        tc = kwargs.get('transcoder')
        if isinstance(tc, type):
            kwargs['transcoder'] = tc()

        if _gevent_support:
            kwargs['_iops'] = SelectIOPS()

        super(Connection, self).__init__(**kwargs)
        for ctl, val in _cntlopts.items():
            self._cntl(ctl, val)

        try:
            self._do_ctor_connect()
        except exceptions.CouchbaseError as e:
            if not _no_connect_exceptions:
                raise

    def _do_ctor_connect(self):
        """
        This should be overidden by subclasses which want to use a different
        sort of connection behavior
        """
        self._connect()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def errors(self, clear_existing=True):
        """
        Get miscellaneous error information.

        This function returns error information relating to the client
        instance. This will contain error information not related to
        any specific operation and may also provide insight as to what
        caused a specific operation to fail.

        :param boolean clear_existing: If set to true, the errors will be
          cleared once they are returned. The client will keep a history of
          the last 1000 errors which were received.

        :return: a tuple of ((errnum, errdesc), ...) (which may be empty)
        """

        ret = tuple(self._errors)

        if clear_existing:
            self._errors.clear()
        return ret

    def pipeline(self):
        """

        Returns a new :class:`Pipeline` context manager. When the context
        manager is active, operations performed will return ``None``, and
        will be sent on the network when the context leaves (in its
        ``__exit__`` method). To get the results of the pipelined operations,
        inspect the :attr:`Pipeline.results` property.

        Operational errors (i.e. negative replies from the server, or network
        errors) are delivered when the pipeline exits, but argument errors
        are thrown immediately.

        :return: a :class:`Pipeline` object

        :raise: :exc:`couchbase.exceptions.PipelineError` if a pipeline
          is already in progress

        :raise: Other operation-specific errors.

        Scheduling multiple operations, without checking results::

          with cb.pipeline():
            cb.set("key1", "value1")
            cb.incr("counter")
            cb.add_multi({
              "new_key1" : "new_value_1",
              "new_key2" : "new_value_2"
            })

        Retrieve the results for several operations::

          pipeline = cb.pipeline()
          with pipeline:
            cb.set("foo", "bar")
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

    # We have these wrappers so that IDEs can do param tooltips and the like.
    # we might move this directly into C some day

    def set(self, key, value, cas=0, ttl=0, format=None,
            persist_to=0, replicate_to=0):
        """Unconditionally store the object in Couchbase.

        :param key: The key to set the value with. By default, the key must be
          either a :class:`bytes` or :class:`str` object encodable as UTF-8.
          If a custom `transcoder` class is used
          (see :meth:`couchbase.Couchbase.connect`), then
          the key object is passed directly to the transcoder, which may
          serialize it how it wishes.
        :type key: string or bytes

        :param value: The value to set for the key. The type for `value`
          follows the same rules as for `key`

        :param int cas: The _CAS_ value to use. If supplied, the value will
          only be stored if it already exists with the supplied CAS

        :param int ttl: Expiration value.
          See :ref:`exp_info` for accepted values.

        :param int format: If specified, indicates the `format` to use when
          encoding the value. If none is specified, it will use the
          `default_format`
          For more info see
          :attr:`~couchbase.connection.Connection.default_format`

        :param int persist_to: Perform durability checking on this many

          .. versionadded:: 1.1.0

          nodes for persistence to disk.
          See :meth:`endure` for more information

        :param int replicate_to: Perform durability checking on this many

          .. versionadded:: 1.1.0

          replicas for presence in memory. See :meth:`endure` for more
          information.

        :raise: :exc:`couchbase.exceptions.ArgumentError` if an
          argument is supplied that is not applicable in this context.
          For example setting the CAS as a string.
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed
        :raise: :exc:`couchbase.exceptions.KeyExistsError` if the key
          already exists on the server with a different CAS value.
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the
          value cannot be serialized with chosen encoder, e.g. if you
          try to store a dictionaty in plain mode.

        :return: :class:`~couchbase.result.Result`

        Simple set::

            cb.set('key', 'value')

        Force JSON document format for value::

            cb.set('foo', {'bar': 'baz'}, format=couchbase.FMT_JSON)

        Perform optimistic locking by specifying last known CAS version::

            cb.set('foo', 'bar', cas=8835713818674332672)

        Several sets at the same time (mutli-set)::

            cb.set_multi({'foo': 'bar', 'baz': 'value'})

        .. seealso:: :meth:`set_multi`

        """
        return _Base.set(self, key, value, cas, ttl, format,
                         persist_to, replicate_to)

    def add(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        """
        Store an object in Couchbase unless it already exists.

        Follows the same conventions as
        :meth:`~couchbase.connection.Connection.set` but the value is
        stored only if it does not exist already. Conversely, the value
        is not stored if the key already exists.

        Notably missing from this method is the `cas` parameter, this is
        because `add` will only succeed if a key does not already exist
        on the server (and thus can have no CAS)

        :raise: :exc:`couchbase.exceptions.KeyExistsError` if the key
          already exists

        .. seealso:: :meth:`set`, :meth:`add_multi`

        """
        return _Base.add(self, key, value, ttl=ttl, format=format,
                         persist_to=persist_to, replicate_to=replicate_to)

    def replace(self, key, value, cas=0, ttl=0, format=None,
                persist_to=0, replicate_to=0):
        """
        Store an object in Couchbase only if it already exists.

        Follows the same conventions as
        :meth:`~couchbase.connection.Connection.set`, but the value is
        stored only if a previous value already exists.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist

        .. seealso:: :meth:`set`, :meth:`replace_multi`

        """
        return _Base.replace(self, key, value, ttl=ttl, cas=cas, format=format,
                             persist_to=persist_to, replicate_to=replicate_to)

    def append(self, key, value, cas=0, ttl=0, format=None,
               persist_to=0, replicate_to=0):
        """
        Append a string to an existing value in Couchbase.

        This follows the same conventions as
        :meth:`~couchbase.connection.Connection.set`.

        The `format` argument must be one of :const:`~couchbase.FMT_UTF8` or
        :const:`~couchbase.FMT_BYTES`. If not specified, it will be
        :const:`~couchbase.FMT_UTF8`
        (overriding the :attr:`default_format` attribute).
        This is because JSON or Pickle formats will be nonsensical when
        random data is appended to them. If you wish to modify a JSON or
        Pickle encoded object, you will need to retrieve it (via :meth:`get`),
        modify it, and then store it again (using :meth:`set`).

        Additionally, you must ensure the value (and flags) for the current
        value is compatible with the data to be appended. For an example,
        you may append a :const:`~couchbase.FMT_BYTES` value to an existing
        :const:`~couchbase.FMT_JSON` value, but an error will be thrown when
        retrieving the value using
        :meth:`get` (you may still use the :attr:`data_passthrough` to
        overcome this).

        :raise: :exc:`couchbase.exceptions.NotStoredError` if the key does
          not exist

        .. seealso::
            :meth:`set`, :meth:`append_multi`

        """
        return _Base.append(self, key, value, ttl=ttl, cas=cas, format=format,
                            persist_to=persist_to, replicate_to=replicate_to)

    def prepend(self, key, value, cas=0, ttl=0, format=None,
                persist_to=0, replicate_to=0):
        """
        Prepend a string to an existing value in Couchbase.

        .. seealso::
            :meth:`append`, :meth:`prepend_multi`

        """
        return _Base.prepend(self, key, value, ttl=ttl, cas=cas, format=format,
                             persist_to=persist_to, replicate_to=replicate_to)

    def get(self, key, ttl=0, quiet=None, replica=False, no_format=False):
        """Obtain an object stored in Couchbase by given key.

        :param string key: The key to fetch. The type of key is the same
          as mentioned in :meth:`set`

        :param int ttl:
          If specified, indicates that the key's expiration time should be
          *modified* when retrieving the value. See also :ref:`exp_info`

        :param boolean quiet: causes `get` to return None instead of
          raising an exception when the key is not found. It defaults
          to the value set by
          :attr:`~couchbase.connection.Connection.quiet` on the instance.
          In `quiet` mode, the error may still be obtained by inspecting
          the :attr:`~couchbase.result.Result.rc` attribute of the
          :class:`couchbase.result.Result` object, or
          checking :attr:`couchbase.result.Result.success`.

          Note that the default value is `None`, which means to use
          the :attr:`quiet`. If it is a boolean (i.e. `True` or `False) it will
          override the :class:`Connection`-level :attr:`quiet` attribute.

        :param bool replica: Whether to fetch this key from a replica
          rather than querying the master server. This is primarily useful
          when operations with the master fail (possibly due to a configuration
          change). It should normally be used in an exception handler like so

          Using the ``replica`` option::

            try:
                res = c.get("key", quiet=True) # suppress not-found errors
            catch CouchbaseError:
                res = c.get("key", replica=True, quiet=True)


        :param bool no_format:

          .. versionadded:: 1.1.0

          If set to ``True``, then the value will always be
          delivered in the :class:`~couchbase.result.Result` object as being of
          :data:`~couchbase.FMT_BYTES`. This is a item-local equivalent of using
          the :attr:`data_passthrough` option


        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          is missing in the bucket
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed
        :raise: :exc:`couchbase.exceptions.ValueFormatError` if the
          value cannot be deserialized with chosen decoder, e.g. if you
          try to retreive an object stored with an unrecognized format
        :return: A :class:`~couchbase.result.Result` object

        Simple get::

            value = cb.get('key').value

        Get multiple values::

            cb.get_multi(['foo', 'bar'])
            # { 'foo' : <Result(...)>, 'bar' : <Result(...)> }

        Inspect the flags::

            rv = cb.get("key")
            value, flags, cas = rv.value, rv.flags, rv.cas

        Update the expiration time::

            rv = cb.get("key", int(time.time()) + 10)
            # Expires in ten seconds


        .. seealso::
            :meth:`get_multi`

        """

        return _Base.get(self, key, ttl, quiet, replica, no_format)

    def touch(self, key, ttl=0):
        """Update a key's expiration time

        :param string key: The key whose expiration time should be modified
        :param int ttl: The new expiration time. If the expiration time is
          ``0`` then the key never expires (and any existing expiration is
          removed). See :ref:`exp_info` for more details about expiration

        :return: :class:`couchbase.result.OperationResult`

        Update the expiration time of a key ::

            cb.set("key", ttl=int(time.time()) + 100)
            # expires in 100 seconds
            cb.touch("key", ttl=0)
            # key should never expire now

        :raise: The same things that :meth:`get` does

        .. seealso::
            :meth:`get` - which can be used to get *and* update the expiration,
            :meth:`touch_multi`
        """
        return _Base.touch(self, key, ttl=ttl)

    def lock(self, key, ttl=0):
        """Lock and retrieve a key-value entry in Couchbase.

        :param key: A string which is the key to lock.
        :param int: a TTL for which the lock should be valid.
          While the lock is active, attempts to access the key (via
          other :meth:`lock`, :meth:`set` or other mutation calls) will
          fail with an :exc:`couchbase.exceptions.TemporaryFailError`.
          Note that the value for this option is limited by the maximum allowable
          lock time determined by the server (currently, this is 15 seconds). If
          passed a higher value, the server will silently lower this to its
          maximum limit.

          Note that the TTL value for lock is an offset in seconds, rather than a
          Unix timestamp.


        This function otherwise functions similarly to :meth:`get`;
        specifically, it will return the value upon success.
        Note the :attr:`~couchbase.result.Result.cas` value from the
        :class:`couchbase.result.Result`
        object. This will be needed to :meth:`unlock` the key.

        Note the lock will also be implicitly released if modified by one
        of the :meth:`set` family of functions when the valid CAS is
        supplied

        :raise: :exc:`couchbase.exceptions.TemporaryFailError` if the key
          was already locked.

        :raise: See :meth:`get` for possible exceptions


        Lock a key ::

            rv = cb.lock("locked_key", ttl=5)
            # This key is now locked for the next 5 seconds.
            # attempts to access this key will fail until the lock
            # is released.

            # do important stuff...

            cb.unlock("locked_key", rv.cas)

        Lock a key, implicitly unlocking with :meth:`set` with CAS ::

            rv = self.cb.lock("locked_key", ttl=5)
            new_value = rv.value.upper()
            cb.set("locked_key", new_value, rv.cas)


        Poll and Lock ::

            rv = None
            begin_time = time.time()
            while time.time() - begin_time < 15:
                try:
                    rv = cb.lock("key", ttl=10)
                except TemporaryFailError:
                    print("Key is currently locked.. waiting")
                    time.sleep(1)

            if not rv:
                raise Exception("Waited too long..")

            # Do stuff..

            cb.unlock("key", rv.cas)

        .. seealso::
            :meth:`get`, :meth:`lock_multi`, :meth:`unlock`

        """
        return _Base.lock(self, key, ttl=ttl)

    def unlock(self, key, cas):
        """Unlock a Locked Key in Couchbase.

        :param key: The key to unlock
        :param cas: The cas returned from
          :meth:`lock`'s :class:`couchbase.result.Result` object.


        Unlock a previously-locked key in Couchbase. A key is
        locked by a call to :meth:`lock`.


        See :meth:`lock` for an example.

        :raise: :exc:`couchbase.exceptions.KeyExistsError` if the CAS
          supplied does not match the CAS on the server (possibly because
          it was unlocked by previous call).

        .. seealso::

        :meth:`lock`
        :meth:`unlock_multi`

        """
        return _Base.unlock(self, key, cas=cas)

    def delete(self, key, cas=0, quiet=None, persist_to=0, replicate_to=0):
        """Remove the key-value entry for a given key in Couchbase.

        :param key: A string which is the key to delete. The format and type
          of the key follows the same conventions as in :meth:`set`

        :type key: string, dict, or tuple/list
        :param int cas: The CAS to use for the removal operation.
          If specified, the key will only be deleted from the server if
          it has the same CAS as specified. This is useful to delete a
          key only if its value has not been changed from the version
          currently visible to the client.
          If the CAS on the server does not match the one specified,
          an exception is thrown.
        :param boolean quiet:
          Follows the same semantics as `quiet` in :meth:`get`

        :param int persist_to: If set, wait for the item to be deleted from
          the storage of at least these many nodes

          .. versionadded:: 1.2.0

        :param int replicate_to: If set, wait for the item to be deleted from
          the cache of at least these many nodes (excluding the master)

          .. versionadded:: 1.2.0

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist on the bucket
        :raise: :exc:`couchbase.exceptions.KeyExistsError` if a CAS
          was specified, but the CAS on the server had changed
        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection was closed

        :return: A :class:`~couchbase.result.Result` object.


        Simple delete::

            ok = cb.delete("key").success

        Don't complain if key does not exist::

            ok = cb.delete("key", quiet=True)

        Only delete if CAS matches our version::

            rv = cb.get("key")
            cb.delete("key", cas=rv.cas)

        Remove multiple keys::

            oks = cb.delete_multi(["key1", "key2", "key3"])

        Remove multiple keys with CAS::

            oks = cb.delete({
                "key1" : cas1,
                "key2" : cas2,
                "key3" : cas3
            })


        .. seealso:: :meth:`delete_multi`, :meth:`endure` for more information
          on the ``persist_to`` and ``replicate_to`` options.

        """
        return _Base.delete(self, key, cas, quiet, persist_to=persist_to,
                            replicate_to=replicate_to)

    def incr(self, key, amount=1, initial=None, ttl=0):
        """
        Increment the numeric value of a key.

        :param string key: A key whose counter value is to be incremented

        :param int amount: an amount by which the key should be
          incremented

        :param initial: The initial value for the key, if it does not
          exist. If the key does not exist, this value is used, and
          `amount` is ignored. If this parameter is `None` then no
          initial value is used
        :type initial: int or `None`

        :param int ttl: The lifetime for the key, after which it will
          expire. See :ref:`exp_info`.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist on the bucket (and `initial` was `None`)

        :raise: :exc:`couchbase.exceptions.DeltaBadvalError` if the key
          exists, but the existing value is not numeric

        :return:
          A :class:`couchbase.result.Result` object. The current value
          of the counter may be obtained by inspecting the return value's
          `value` attribute.

        Simple increment::

            rv = cb.incr("key")
            rv.value
            # 42

        Increment by 10::

            ok = cb.incr("key", amount=10)

        Increment by 20, set initial value to 5 if it does not exist::

            ok = cb.incr("key", amount=20, initial=5)

        Increment three keys::

            kv = cb.incr_multi(["foo", "bar", "baz"])
            for key, result in kv.items():
                print "Key %s has value %d now" % (key, result.value)

        .. seealso:: :meth:`decr`, :meth:`incr_multi`

        """
        return _Base.incr(self, key, amount, initial, ttl)

    def decr(self, key, amount=1, initial=None, ttl=0):
        """
        Like :meth:`incr`, but decreases, rather than increaes the
        counter value

        .. seealso:: :meth:`incr`, :meth:`decr_multi`

        """
        return _Base.decr(self, key, amount, initial, ttl)

    def stats(self, keys=None):
        """Request server statistics
        Fetches stats from each node in the cluster. Without a key
        specified the server will respond with a default set of
        statistical information. It returns the a `dict` with stats keys
        and node-value pairs as a value.

        :param stats: One or several stats to query
        :type stats: string or list of string

        :raise: :exc:`couchbase.exceptions.ConnectError` if the
          connection closed

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
        return self._stats(keys)

    def observe(self, key, master_only=False):
        """
        Return storage information for a key.
        The ``observe`` function maps to the low-level ``OBSERVE``
        command.

        It returns a :class:`couchbase.result.ValueResult`
        object with the ``value`` field
        set to a list of :class:`~couchbase.result.ObserveInfo`
        objects. Each element in the list
        responds to the storage status for the key on the given node. The
        length of the list (and thus the number of
        :class:`~couchbase.result.ObserveInfo` objects)
        are equal to the number of online replicas plus the master for the
        given key.

        :param string key: The key to inspect
        :param bool master_only: Whether to only retrieve information from
          the master node. Note this requires libcouchbase 2.3.0 or greater

        .. seealso:: :ref:`observe_info`

        """
        return _Base.observe(self, key, master_only)

    def endure(self, key, persist_to=-1, replicate_to=-1,
               cas=0,
               check_removed=False,
               timeout=5.0,
               interval=0.010):
        """
        Wait until a key has been distributed to one or more nodes

        .. versionadded:: 1.1.0

        By default, when items are stored to Couchbase, the operation is
        considered successful if the vBucket master (i.e. the "primary" node)
        for the key has successfuly stored the item in its memory.

        In most situations, this is sufficient to assume that the item has
        successfuly been stored. However the possibility remains that the
        "master" server will go offline as soon as it sends back the successful
        response and the data is lost.

        The ``endure`` function allows you to provide stricter criteria for
        success. The criteria may be expressed in terms of number of nodes
        for which the item must exist in that node's RAM and/or on that node's
        disk. Ensuring that an item exists in more than one place is a safer
        way to guarantee against possible data loss.

        We call these requirements `Durability Constraints`, and thus the
        method is called `endure`.

        :param string key: The key to endure.
        :param int persist_to: The minimum number of nodes which must contain
            this item on their disk before this function returns. Ensure that
            you do not specify too many nodes; otherwise this function will
            fail. Use the :attr:`server_nodes` to determine how many nodes
            exist in the cluster.

            The maximum number of nodes an item can reside on is currently
            fixed to 4 (i.e. the "master" node, and up to three "replica"
            nodes). This limitation is current as of Couchbase Server version
            2.1.0.

            If this parameter is set to a negative value, the maximum number
            of possible nodes the key can reside on will be used.

        :param int replicate_to: The minimum number of replicas which must
            contain this item in their memory for this method to succeed.
            As with ``persist_to``, you may specify a negative value in which
            case the requirement will be set to the maximum number possible.

        :param float timeout: A timeout value in seconds before this function
            fails with an exception. Typically it should take no longer than
            several milliseconds on a functioning cluster for durability
            requirements to be satisfied (unless something has gone wrong).

        :param float interval: The polling interval in secods
            to use for checking the
            key status on the respective nodes. Internally, ``endure`` is
            implemented by polling each server individually to see if the
            key exists on that server's disk and memory. Once the status
            request is sent to all servers, the client will check if their
            replies are satisfactory; if they are then this function succeeds,
            otherwise the client will wait a short amount of time and try
            again. This parameter sets this "wait time".

        :param bool check_removed: This flag inverts the check. Instead of
            checking that a given key *exists* on the nodes, this changes
            the behavior to check that the key is *removed* from the nodes.

        :param long cas: The CAS value to check against. It is possible for
            an item to exist on a node but have a CAS value from a prior
            operation. Passing the CAS ensures that only replies from servers
            with a CAS matching this parameter are accepted

        :return: A :class:`~couchbase.result.OperationResult`

        :raise: :exc:`~couchbase.exceptions.CouchbaseError`.
            see :meth:`set` and :meth:`get` for possible errors

        .. seealso:: :meth:`set`, :meth:`endure_multi`
        """
        # We really just wrap 'endure_multi'
        kv = { key : cas }
        rvs = self.endure_multi(keys=kv,
                                persist_to=persist_to,
                                replicate_to=replicate_to,
                                check_removed=check_removed,
                                timeout=timeout,
                                interval=interval)
        return rvs[key]

    def durability(self, persist_to=-1, replicate_to=-1, timeout=0.0):
        """
        Returns a context manager which will apply the given
        persistence/replication settings to all mutation operations when
        active

        :param int persist_to:
        :param int replicate_to:

        See :meth:`endure` for the meaning of these two values

        Thus, something like::

          with cb.durability(persist_to=3):
            cb.set("foo", "foo_value")
            cb.set("bar", "bar_value")
            cb.set("baz", "baz_value")

        is equivalent to::

            cb.set("foo", "foo_value", persist_to=3)
            cb.set("bar", "bar_value", persist_to=3)
            cb.set("baz", "baz_value", persist_to=3)


        .. versionadded:: 1.2.0

        .. seealso:: :meth:`endure`
        """
        return DurabilityContext(self, persist_to, replicate_to, timeout)

    def set_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        """Set multiple keys

        This follows the same semantics as
        :meth:`~couchbase.connection.Connection.set`

        :param dict keys: A dictionary of keys to set. The keys are the keys
          as they should be on the server, and the values are the values for
          the keys to be stored.


          From version 1.1.0, `keys` may also be a
          :class:`~couchbase.items.ItemCollection`. If using a dictionary
          variant for item collections, an additional `ignore_cas` parameter
          may be supplied with a boolean value. If not specified, the operation
          will fail if the CAS value on the server does not match the one
          specified in the `Item`'s `cas` field.

        :param int ttl: If specified, sets the expiration value for all
          keys. See :ref:`exp_info`

        :param int format:
          If specified, this is the conversion format which will be used for
          _all_ the keys.

        :param int persist_to: Durability constraint for persistence.
          Note that it is more efficient to use :meth:`endure_multi`
          on the returned :class:`~couchbase.result.MultiResult` than
          using these parameters for a high volume of keys. Using these
          parameters however does save on latency as the constraint checking
          for each item is performed as soon as it is successfully stored.

        :param int replicate_to: Durability constraints for replication.
          See notes on the `persist_to` parameter for usage.

        :return: A :class:`~couchbase.result.MultiResult` object, which
          is a `dict` subclass.

        The multi methods are more than just a convenience, they also save on
        network performance by batch-scheduling operations, reducing latencies.
        This is especially noticeable on smaller value sizes.

        .. seealso:: :meth:`set`

        """
        return _Base.set_multi(self, keys, ttl=ttl, format=format,
                               persist_to=persist_to, replicate_to=replicate_to)

    def add_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        """Add multiple keys.
        Multi variant of :meth:`~couchbase.connection.Connection.add`

        .. seealso:: :meth:`add`, :meth:`set_multi`, :meth:`set`

        """
        return _Base.add_multi(self, keys, ttl=ttl, format=format,
                               persist_to=persist_to, replicate_to=replicate_to)

    def replace_multi(self, keys, ttl=0, format=None,
                      persist_to=0, replicate_to=0):
        """Replace multiple keys.
        Multi variant of :meth:`replace`

        .. seealso:: :meth:`replace`, :meth:`set_multi`, :meth:`set`

        """
        return _Base.replace_multi(self, keys, ttl=ttl, format=format,
                                   persist_to=persist_to,
                                   replicate_to=replicate_to)

    def append_multi(self, keys, ttl=0, format=None,
                     persist_to=0, replicate_to=0):
        """Append to multiple keys.
        Multi variant of :meth:`append`.


        .. warning::

            If using the `Item` interface, use the :meth:`append_items`
            and :meth:`prepend_items` instead, as those will automatically
            update the :attr:`couchbase.items.Item.value` property upon
            successful completion.

        .. seealso:: :meth:`append`, :meth:`set_multi`, :meth:`set`

        """
        return _Base.append_multi(self, keys, ttl=ttl, format=format,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to)

    def prepend_multi(self, keys, ttl=0, format=None,
                      persist_to=0, replicate_to=0):
        """Prepend to multiple keys.
        Multi variant of :meth:`prepend`

        .. seealso:: :meth:`prepend`, :meth:`set_multi`, :meth:`set`

        """
        return _Base.prepend_multi(self, keys, ttl=ttl, format=format,
                                   persist_to=persist_to,
                                   replicate_to=replicate_to)

    def get_multi(self, keys, ttl=0, quiet=None, replica=False, no_format=False):
        """Get multiple keys
        Multi variant of :meth:`get`

        :param keys: keys the keys to fetch
        :type keys: :ref:`iterable<argtypes>`

        :param int ttl: Set the expiration for all keys when retrieving.
          See :ref:`exp_info`

        :param boolean replica:
          Whether the results should be obtained from a replica instead of the
          master. See :meth:`get` for more information about this parameter.

        :return: A :class:`~couchbase.result.MultiResult` object.
          This object is a subclass of dict and contains the keys (passed as)
          `keys` as the dictionary keys, and
          :class:`~couchbase.result.Result` objects as values

        """
        return _Base.get_multi(self, keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)

    def touch_multi(self, keys, ttl=0):
        """Touch multiple keys

        Multi variant of :meth:`touch`

        :param keys: the keys to touch
        :type keys: :ref:`iterable<argtypes>`

        ``keys`` can also be a dictionary with values being integers, in
        whic case the value for each key will be used as the TTL instead
        of the global one (i.e. the one passed to this function)

        :param int ttl: The new expiration time. See :ref:`exp_info`

        :return: A :class:`~couchbase.result.MultiResult` object


        Update three keys to expire in 10 seconds ::

            cb.touch_multi(("key1", "key2", "key3"), ttl=int(time.time())+10)

        Update three keys with different expiration times ::
            now = int(time.time())
            cb.touch_multi({"foo" : now+1, "bar" : now+5, "baz" : now+10})

        .. seealso:: :meth:`touch`
        """
        return _Base.touch_multi(self, keys, ttl=ttl)

    def lock_multi(self, keys, ttl=0):
        """Lock multiple keys

        Multi variant of :meth:`lock`

        :param keys: the keys to lock
        :type keys: :ref:`iterable<argtypes>`
        :param int ttl: The lock timeout for all keys

        :return: a :class:`~couchbase.result.MultiResult` object

        .. seealso:: :meth:`lock`

        """
        return _Base.lock_multi(self, keys, ttl=ttl)

    def unlock_multi(self, keys):
        """Unlock multiple keys

        Multi variant of :meth:`unlock`

        :param dict keys: the keys to unlock

        :return: a :class:`~couchbase.result.MultiResult` object

        The value of the ``keys`` argument should be either the CAS, or a
        previously returned :class:`Result` object from a :meth:`lock` call.
        Effectively, this means you may pass a
        :class:`~couchbase.result.MultiResult` as the ``keys`` argument.

        Thus, you can do something like ::

            keys = (....)
            rvs = cb.lock_multi(keys, ttl=int(time.time()) + 5)
            # do something with rvs
            cb.unlock_multi(rvs)


        .. seealso:: :meth:`unlock`
        """
        return _Base.unlock_multi(self, keys)

    def observe_multi(self, keys, master_only=False):
        """
        Multi-variant of :meth:`observe`
        """
        return _Base.observe_multi(self, keys, master_only)

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1,
                     timeout=5.0,
                     interval=0.010,
                     check_removed=False):
        """
        .. versionadded:: 1.1.0

        Check durability requirements for multiple keys

        :param keys: The keys to check

        The type of keys may be one of the following:

            * Sequence of keys
            * A :class:`~couchbase.result.MultiResult` object
            * A ``dict`` with CAS values as the dictionary value
            * A sequence of :class:`~couchbase.result.Result` objects

        :return: A :class:`~couchbase.result.MultiResult` object of
            :class:`~couchbase.result.OperationResult` items.

        .. seealso:: :meth:`endure`
        """
        return _Base.endure_multi(self, keys, persist_to, replicate_to,
                                  timeout=timeout,
                                  interval=interval,
                                  check_removed=check_removed)


    def rget(self, key, replica_index=None, quiet=None):
        """
        Get a key from a replica

        :param string key: The key to fetch

        :param int replica_index: The replica index to fetch.
          If this is ``None`` then this method will return once any replica
          responds. Use :attr:`configured_replica_count` to figure out the
          upper bound for this parameter.

          The value for this parameter must be a number between 0 and the
          value of :attr:`configured_replica_count`-1.

        :param boolean quiet: Whether to suppress errors when the key is not
          found

        This function (if `replica_index` is not supplied) functions like
        the :meth:`get` method that has been passed the `replica` parameter::

            c.get(key, replica=True)

        .. seealso::
            :meth:`get` :meth:`rget_multi`
        """
        if replica_index is not None:
            return _Base._rgetix(self, key, replica=replica_index, quiet=quiet)
        else:
            return _Base._rget(self, key, quiet=quiet)

    def rget_multi(self, keys, replica_index=None, quite=None):
        if replica_index is not None:
            return _Base._rgetix_multi(self, keys, replica=replica_index, quiet=quiet)
        else:
            return _Base._rget_multi(self, keys, quiet=quiet)


    def _view(self, ddoc, view,
              use_devmode=False,
              params=None,
              unrecognized_ok=False,
              passthrough=False):
        """
        .. warning:: This method's API is not stable

        Execute a view (MapReduce) query

        :param string ddoc: Name of the design document
        :param string view: Name of the view function to execute
        :param params: Extra options to pass to the view engine
        :type params: string or dict

        :return: a :class:`~couchbase.result.HttpResult` object.
        """

        if params:
            if not isinstance(params, str):
                params = make_options_string(
                    params,
                    unrecognized_ok=unrecognized_ok,
                    passthrough=passthrough)
        else:
            params = ""

        ddoc = self._mk_devmode(ddoc, use_devmode)
        url = make_dvpath(ddoc, view) + params

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                 path=url,
                                 method=_LCB.LCB_HTTP_METHOD_GET,
                                 response_format=FMT_JSON)
        return ret

    def _doc_rev(self, res):
        """
        Returns the rev id from the header
        """
        jstr = res.headers['X-Couchbase-Meta']
        jobj = json.loads(jstr)
        return jobj['rev']

    def _design_poll(self, name, mode, oldres, timeout=5, use_devmode=False):
        """
        Poll for an 'async' action to be complete.
        :param string name: The name of the design document
        :param string mode: One of ``add`` or ``del`` to indicate whether
            we should check for addition or deletion of the document
        :param oldres: The old result from the document's previous state, if
            any
        :param float timeout: How long to poll for. If this is 0 then this
            function returns immediately
        :type oldres: :class:`~couchbase.result.HttpResult`
        """

        if not timeout:
            return True

        if timeout < 0:
            raise ArgumentError.pyexc("Interval must not be negative")

        t_end = time.time() + timeout
        old_rev = None

        if oldres:
            old_rev = self._doc_rev(oldres)

        while time.time() < t_end:
            try:
                cur_resp = self.design_get(name, use_devmode=use_devmode)
                if old_rev and self._doc_rev(cur_resp) == old_rev:
                    continue

                # Try to execute a view..
                vname = list(cur_resp.value['views'].keys())[0]
                try:
                    self._view(name, vname, use_devmode=use_devmode,
                               params={'limit': 1, 'stale': 'ok'})
                    # We're able to query it? whoopie!
                    return True

                except CouchbaseError:
                    continue

            except CouchbaseError as e:
                if mode == 'del':
                    # Deleted, whopee!
                    return True
                cur_resp = e.objextra

        raise exceptions.TimeoutError.pyexc(
            "Wait time for design action completion exceeded")

    def _mk_devmode(self, n, use_devmode):
        if n.startswith("dev_") or not use_devmode:
            return n
        return "dev_" + n

    def design_create(self, name, ddoc, use_devmode=True, syncwait=0):
        """
        Store a design document

        :param string name: The name of the design
        :param ddoc: The actual contents of the design document

        :type ddoc: string or dict
            If ``ddoc`` is a string, it is passed, as-is, to the server.
            Otherwise it is serialized as JSON, and its ``_id`` field is set to
            ``_design/{name}``.

        :param bool use_devmode:
            Whether a *development* mode view should be used. Development-mode
            views are less resource demanding with the caveat that by default
            they only operate on a subset of the data. Normally a view will
            initially be created in 'development mode', and then published
            using :meth:`design_publish`

        :param float syncwait:
            How long to poll for the action to complete. Server side design
            operations are scheduled and thus this function may return before
            the operation is actually completed. Specifying the timeout here
            ensures the client polls during this interval to ensure the
            operation has completed.

        :raise: :exc:`couchbase.exceptions.TimeoutError` if ``syncwait`` was
            specified and the operation could not be verified within the
            interval specified.

        :return: An :class:`~couchbase.result.HttpResult` object.

        .. seealso:: :meth:`design_get`, :meth:`design_delete`,
            :meth:`design_publish`

        """
        name = self._mk_devmode(name, use_devmode)

        fqname = "_design/{0}".format(name)
        if isinstance(ddoc, dict):
            ddoc = ddoc.copy()
            ddoc['_id'] = fqname
            ddoc = json.dumps(ddoc)
        else:
            if use_devmode:
                raise ArgumentError.pyexc("devmode can only be used "
                                          "with dict type design docs")

        existing = None
        if syncwait:
            try:
                existing = self.design_get(name, use_devmode=False)
            except CouchbaseError:
                pass

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                 path=fqname,
                                 method=_LCB.LCB_HTTP_METHOD_PUT,
                                 post_data=ddoc,
                                 content_type="application/json",
                                 fetch_headers=True)

        self._design_poll(name, 'add', existing, syncwait,
                          use_devmode=use_devmode)
        return ret

    def design_get(self, name, use_devmode=True):
        """
        Retrieve a design document

        :param string name: The name of the design document
        :param bool use_devmode: Whether this design document is still in
            "development" mode

        :return: A :class:`~couchbase.result.HttpResult` containing
            a dict representing the format of the design document

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist.

        .. seealso:: :meth:`design_create`

        """
        name = self._mk_devmode(name, use_devmode)

        existing = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                      path="_design/" + name,
                                      method=_LCB.LCB_HTTP_METHOD_GET,
                                      content_type="application/json",
                                      fetch_headers=True)
        return existing

    def design_publish(self, name, syncwait=0):
        """
        Convert a development mode view into a production mode views.
        Production mode views, as opposed to development views, operate on the
        entire cluster data (rather than a restricted subset thereof).

        :param string name: The name of the view to convert.

        Once the view has been converted, ensure that all functions (such as
        :meth:`design_get`) have the ``use_devmode`` parameter disabled,
        otherwise an error will be raised when those functions are used.

        Note that the ``use_devmode`` option is missing. This is intentional
        as the design document must currently be a development view.

        :return: An :class:`~couchbase.result.HttpResult` object.

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist

        .. seealso:: :meth:`design_create`, :meth:`design_delete`,
            :meth:`design_get`
        """
        existing = self.design_get(name, use_devmode=True)
        rv = self.design_create(name, existing.value, use_devmode=False,
                           syncwait=syncwait)
        self._design_poll(name, 'add', None,
                          timeout=syncwait, use_devmode=False)
        return rv

    def design_delete(self, name, use_devmode=True, syncwait=0):
        """
        Delete a design document

        :param string name: The name of the design document to delete
        :param bool use_devmode: Whether the design to delete is a development
            mode design doc.

        :param float syncwait: Timeout for operation verification. See
            :meth:`design_create` for more information on this parameter.

        :return: An :class:`HttpResult` object.

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist
        :raise: :exc:`couchbase.exceptions.TimeoutError` if ``syncwait`` was
            specified and the operation could not be verified within the
            specified interval.

        .. seealso:: :meth:`design_create`, :meth:`design_get`

        """
        name = self._mk_devmode(name, use_devmode)
        existing = None
        if syncwait:
            try:
                existing = self.design_get(name, use_devmode=False)
            except CouchbaseError:
                pass

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                 path="_design/" + name,
                                 method=_LCB.LCB_HTTP_METHOD_DELETE,
                                 fetch_headers=True)

        self._design_poll(name, 'del', existing, syncwait)
        return ret

    def query(self, design, view, use_devmode=False, itercls=View, **kwargs):
        """
        Query a pre-defined MapReduce view, passing parameters.

        This method executes a view on the cluster. It accepts various
        parameters for the view and returns an iterable object (specifically,
        a :class:`~couchbase.views.iterator.View`).

        :param string design: The design document
        :param string view: The view function contained within the design
            document
        :param boolean use_devmode: Whether the view name should be transformed
            into a development-mode view. See documentation on
            :meth:`design_create` for more explanation.

        :param kwargs: Extra arguments passedd to the
            :class:`~couchbase.views.iterator.View` object constructor.

        :param itercls: Subclass of 'view' to use.

        .. seealso::

            * :class:`~couchbase.views.iterator.View`

                which contains more extensive documentation and examples

            * :class:`~couchbase.views.params.Query`

                which contains documentation on the available query options

        """
        design = self._mk_devmode(design, use_devmode)
        return itercls(self, design, view, **kwargs)

    def __repr__(self):
        return ("<{modname}.{cls} bucket={bucket}, "
                "nodes={nodes} at 0x{oid:x}>"
                ).format(modname=__name__,
                         cls=self.__class__.__name__,
                         nodes=self.server_nodes,
                         bucket=self.bucket,
                         oid=id(self))


    # "items" interface
    def append_items(self, items, **kwargs):
        """
        Method to append data to multiple :class:`~couchbase.items.Item` objects.

        This method differs from the normal :meth:`append_multi` in that each
        `Item`'s `value` field is updated with the appended data upon successful
        completion of the operation.

        :param items: The item dictionary. The value for each key should contain
          a ``fragment`` field containing the object to append to the value on
          the server.

        :type items: :class:`~couchbase.items.ItemOptionDict`.

        The rest of the options are passed verbatim to :meth:`append_multi`

        .. seealso:: :meth:`append_multi`, :meth:`append`
        """
        rv = self.append_multi(items, **kwargs)
        # Assume this is an 'ItemOptionDict'
        for k, v in items.dict.items():
            if k.success:
                k.value += v["fragment"]

        return rv

    def prepend_items(self, items, **kwargs):
        """
        Method to prepend data to multiple :class:`~couchbase.items.Item` objects.

        See :meth:`append_items` for more information

        .. seealso:: :meth:`append_items`
        """
        rv = self.prepend_multi(items, **kwargs)
        for k, v in items.dict.items():
            if k.success:
                k.value = v["fragment"] + k.value

        return rv

    @property
    def closed(self):
        """
        Returns True if the object has been closed with :meth:`_close`
        """
        return self._privflags & _LCB.PYCBC_CONN_F_CLOSED


    """
    Lists the names of all the memcached operations. This is useful
    for classes which want to wrap all the methods
    """
    _MEMCACHED_OPERATIONS = ('set', 'get', 'add', 'append', 'prepend',
                             'replace', 'delete', 'incr', 'decr', 'touch',
                             'lock', 'unlock', 'arithmetic', 'endure',
                             'observe', 'rget', 'stats')

    _MEMCACHED_NOMULTI = ('stats')

    @classmethod
    def _gen_memd_wrappers(cls, factory):
        """
        Generates wrappers for all the memcached operations.
        :param factory: A function to be called to return the wrapped method.
          It will be called with two arguments; the first is the unbound
          method being wrapped, and the second is the name of such a method.

          The factory shall return a new unbound method

        :return: A dictionary of names mapping the API calls to the wrapped
        functions
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


    def _cntl(self, *args, **kwargs):
        """
        Interface to 'lcb_cntl'.

        This method accepts an opcode and an
        optional value. Constants are intentionally not defined for
        the various opcodes to allow saner error handling when an
        unknown opcode is not used.

        .. warning::

          If you pass the wrong parameters to this API call, your
          application may crash. For this reason, this is not a
          public API call. Nevertheless it may be used sparingly as
          a workaround for settings which may have not yet been exposed
          directly via a supported API

        :param int op: Type of cntl to access. These are defined in
          libcouchbase's ``cntl.h`` header file

        :param value: An optional value to supply for the operation.
           If a value is not passed then the operation will return
           the current value of the cntl without doing anything else.
           otherwise, it will interpret the cntl in a manner that
           makes sense. If the value is a float, it will be treated
           as a timeout value and will be multiplied by 1000000 to yield
           the microsecond equivalent for the library. If the value
           is a boolean, it is treated as a C ``int``

        :param value_type: String indicating the type of C-level value to be
           passed to ``lcb_cntl()``. The possible values are:

            * ``"string"`` - NUL-terminated `const char`. Pass a Python string
            * ``"int"`` - C ``int`` type. Pass a Python int
            * ``"uint32_t"`` - C ``lcb_uint32_t`` type. Pass a Python int
            * ``"unsigned"`` - C ``unsigned int`` type. Pass a Python int
            * ``"float"`` - C ``float`` type. Pass a Python float
            * ``"timeout"`` - The number of seconds as a float. This is converted
              into microseconds within the extension library.

        :return: If no `value` argument is provided, retrieves the current
            setting (per the ``value_type`` specification). Otherwise this
            function returns ``None``.

        """
        return _Base._cntl(self, *args, **kwargs)
