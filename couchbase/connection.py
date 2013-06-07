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

from couchbase.exceptions import *
from couchbase.user_constants import *
from couchbase.result import *

import couchbase.exceptions as exceptions
from couchbase.views.params import make_dvpath, make_options_string

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

        if not bucket:
            raise exceptions.ArgumentError("A bucket name must be given")


        kwargs['host'] = self._gen_host_string(host, port)
        kwargs['bucket'] = bucket

        if password and not username:
            kwargs['username'] = bucket


        # Internal parameters
        kwargs['_errors'] = deque(maxlen=1000)

        try:
            super(Connection, self).__init__(**kwargs)

        except exceptions.CouchbaseError as e:
            if not _no_connect_exceptions:
                raise

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

    # We have these wrappers so that IDEs can do param tooltips and the like.
    # we might move this directly into C some day

    def set(self, key, value, cas=0, ttl=0, format=None):
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

        :param int cas: The _CAS_ value to use. If supplied, the value will only
          be stored if it already exists with the supplied CAS

        :param int ttl: If specified, the key will expire after this many
          seconds

        :param int format: If specified, indicates the `format` to use when
          encoding the value. If none is specified, it will use the
          `default_format`
          For more info see
          :attr:`~couchbase.connection.Connection.default_format`

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

        .. seealso::

        :meth:`set_multi`

        """
        return _Base.set(self, key, value, cas, ttl, format)

    def add(self, key, value, ttl=0, format=None):
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

        .. seealso::

        :meth:`set`
        :meth:`add_multi`

        """
        return _Base.add(self, key, value, ttl=ttl, format=format)

    def replace(self, key, value, cas=0, ttl=0, format=None):
        """
        Store an object in Couchbase only if it already exists.

        Follows the same conventions as
        :meth:`~couchbase.connection.Connection.set`, but the value is
        stored only if a previous value already exists.

        :raise: :exc:`couchbase.exceptions.NotFoundError` if the key
          does not exist

        .. seealso::

        :meth:`set`
        :meth:`replace_multi`

        """
        return _Base.replace(self, key, value, ttl=ttl, cas=cas, format=format)

    def append(self, key, value, cas=0, ttl=0, format=None):
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

        :meth:`set`
        :meth:`append_multi`

        """
        return _Base.append(self, key, value, ttl=ttl, cas=cas, format=format)

    def prepend(self, key, value, cas=0, ttl=0, format=None):
        """
        Prepend a string to an existing value in Couchbase.

        .. seealso::

        :meth:`append`
        :meth:`prepend_multi`

        """
        return _Base.prepend(self, key, value, ttl=ttl, cas=cas, format=format)

    def get(self, key, ttl=0, quiet=None):
        """Obtain an object stored in Couchbase by given key.

        :param string key: The key to fetch. The type of key is the same
          as mentioned in :meth:`set`

        :param int ttl:
          If specified, indicates that the key's expiration time should be
          *modified* when retrieving the value.

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

            rv = cb.get("key", ttl=10)
            # Expires in ten seconds


        .. seealso::

        :meth:`get_multi`

        """

        return _Base.get(self, key, ttl, quiet)

    def touch(self, key, ttl=0):
        """Update a key's expiration time

        :param string key: The key whose expiration time should be modified
        :param int ttl: The new expiration time. If the expiration time is ``0``
          then the key never expires (and any existing expiration is removed)

        :return: :class:`couchbase.result.OperationResult`

        Update the expiration time of a key ::

            cb.set("key", ttl=100)
            # expires in 100 seconds
            cb.touch("key", ttl=0)
            # key should never expire now

        :raise: The same things that :meth:`get` does

        .. seealso::

        :meth:`get` - which can be used to get *and* update the expiration

        :meth:`touch_multi`
        """
        return _Base.touch(self, key, ttl=ttl)

    def lock(self, key, ttl=0):
        """Lock and retrieve a key-value entry in Couchbase.

        :param key: A string which is the key to lock.
        :param int: a TTL for which the lock should be valid. If set to
          `0` it will use the default lock timeout on the server.
          While the lock is active, attempts to access the key (via
          other :meth:`lock`, :meth:`set` or other mutation calls) will
          fail with an :exc:`couchbase.exceptions.TemporaryFailError`


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

            rv = cb.lock("locked_key", ttl=100)
            # This key is now locked for the next 100 seconds.
            # attempts to access this key will fail until the lock
            # is released.

            # do important stuff...

            cb.unlock("locked_key", rv.cas)

        Lock a key, implicitly unlocking with :meth:`set` with CAS ::

            rv = self.cb.lock("locked_key", ttl=100)
            new_value = rv.value.upper()
            cb.set("locked_key", new_value, rv.cas)


        Poll and Lock ::

            rv = None
            begin_time = time.time()
            while time.time() - begin_time < 15:
                try:
                    rv = cb.lock("key")
                except TemporaryFailError:
                    print("Key is currently locked.. waiting")
                    time.sleep(0)

            if not rv:
                raise Exception("Waited too long..")

            # Do stuff..

            cb.unlock("key", rv.cas)

        .. seealso::

        :meth:`get`
        :meth:`lock_multi`
        :meth:`unlock`

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


    def delete(self, key, cas=0, quiet=None):
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


        .. seealso::

        :meth:`delete_multi`

        """
        return _Base.delete(self, key, cas, quiet)

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
          expire

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

        .. seealso::

        :meth:`decr`
        :meth:`incr_multi`

        """
        return _Base.incr(self, key, amount, initial, ttl)

    def decr(self, key, amount=1, initial=None, ttl=0):
        """
        Like :meth:`incr`, but decreases, rather than increaes the
        counter value

        .. seealso::

        :meth:`incr`
        :meth:`decr_multi`

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

    def observe(self, key):
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

        .. seealso::
            :ref:`observe_info`

        """
        return _Base.observe(self, key)

    def set_multi(self, keys, ttl=0, format=None):
        """Set multiple keys

        This follows the same semantics as
        :meth:`~couchbase.connection.Connection.set`

        :param dict keys: A dictionary of keys to set. The keys are the keys
          as they should be on the server, and the values are the values for
          the keys to be stored

        :param int ttl: If specified, sets the expiration value for all
          keys

        :param int format:
          If specified, this is the conversion format which will be used for
          _all_ the keys.

        :return: A :class:`~couchbase.result.MultiResult` object, which
          is a `dict` subclass.

        The multi methods are more than just a convenience, they also save on
        network performance by batch-scheduling operations, reducing latencies.
        This is especially noticeable on smaller value sizes.

        .. seealso::

        :meth:`set`

        """
        return _Base.set_multi(self, keys, ttl=ttl, format=format)

    def add_multi(self, keys, ttl=0, format=None):
        """Add multiple keys.
        Multi variant of :meth:`~couchbase.connection.Connection.add`

        .. seealso::

        :meth:`add`
        :meth:`set_multi`
        :meth:`set`

        """
        return _Base.add_multi(self, keys, ttl=ttl, format=format)

    def replace_multi(self, keys, ttl=0, format=None):
        """Replace multiple keys.
        Multi variant of :meth:`replace`

        .. seealso::

        :meth:`replace`
        :meth:`set_multi`
        :meth:`set`

        """
        return _Base.replace_multi(self, keys, ttl=ttl, format=format)

    def append_multi(self, keys, ttl=0, format=None):
        """Append to multiple keys.
        Multi variant of :meth:`append`

        .. seealso::

        :meth:`append`
        :meth:`set_multi`
        :meth:`set`

        """
        return _Base.append_multi(self, keys, ttl=ttl, format=format)

    def prepend_multi(self, keys, ttl=0, format=None):
        """Prepend to multiple keys.
        Multi variant of :meth:`prepend`

        .. seealso::

        :meth:`prepend`
        :meth:`set_multi`
        :meth:`set`

        """
        return _Base.prepend_multi(self, keys, ttl=ttl, format=format)

    def get_multi(self, keys, ttl=0, quiet=None):
        """Get multiple keys
        Multi variant of :meth:`get`

        :param keys: keys the keys to fetch
        :type keys: :ref:`iterable<argtypes>`

        :param int ttl: Set the expiration for all keys when retrieving

        :return: A :class:`~couchbase.result.MultiResult` object.
          This object is a subclass of dict and contains the keys (passed as)
          `keys` as the dictionary keys, and
          :class:`~couchbase.result.Result` objects as values

        """
        return _Base.get_multi(self, keys, ttl=ttl, quiet=quiet)

    def touch_multi(self, keys, ttl=0):
        """Touch multiple keys

        Multi variant of :meth:`touch`

        :param keys: the keys to touch
        :type keys: :ref:`iterable<argtypes>`

        ``keys`` can also be a dictionary with values being integers, in
        whic case the value for each key will be used as the TTL instead
        of the global one (i.e. the one passed to this function)

        :param int ttl: The new expiration time

        :return: A :class:`~couchbase.result.MultiResult` object


        Update three keys to expire in 10 seconds ::

            cb.touch_multi(("key1", "key2", "key3"), ttl=10)

        Update three keys with different expiration times ::

            cb.touch_multi({"foo" : 1, "bar" : 5, "baz" : 10})

        .. seealso::

            :meth:`touch`
        """
        return _Base.touch_multi(self, keys, ttl=ttl)

    def lock_multi(self, keys, ttl=0):
        """Lock multiple keys

        Multi variant of :meth:`lock`

        :param keys: the keys to lock
        :type keys: :ref:`iterable<argtypes>`
        :param int ttl: The lock timeout for all keys

        :return: a :class:`~couchbase.result.MultiResult` object

        .. seealso::

        :meth:`lock`

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
            rvs = cb.lock_multi(keys, ttl=5)
            # do something with rvs
            cb.unlock_multi(rvs)


        .. seealso::

        :meth:`unlock`
        """
        return _Base.unlock_multi(self, keys)

    def observe_multi(self, keys):
        """
        Multi-variant of :meth:`observe`
        """
        return _Base.observe_multi(self, keys)

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

    def _design_poll(self, name, mode, oldres, timeout=5):
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
                cur_resp = self.design_get(name, use_devmode=False)
                if old_rev and self._doc_rev(cur_resp) == old_rev:
                    continue

                # Try to execute a view..
                vname = list(cur_resp.value['views'].keys())[0]
                try:
                    self._view(name, vname, use_devmode=False,
                               params={'limit':1, 'stale':'ok'})
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

        self._design_poll(name, 'add', existing, syncwait)
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

        """
        name = self._mk_devmode(name, use_devmode)

        existing = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                              path="_design/"+name,
                              method=_LCB.LCB_HTTP_METHOD_GET,
                              content_type="application/json",
                              fetch_headers=True)
        return existing

    def design_publish(self, name, syncwait=0):
        """
        Convert a development mode view into a production mode views. Production
        mode views, as opposed to development views, operate on the entire
        cluster data (rather than a restricted subset thereof).

        :param string name: The name of the view to convert.

        Once the view has been converted, ensure that all functions (such as
        :meth:`design_get`) have the ``use_devmode`` parameter disabled,
        otherwise an error will be raised when those functions are used.

        Note that the ``use_devmode`` option is missing. This is intentional
        as the design document must currently be a development view.

        :return: An :class:`~couchbase.result.HttpResult` object.

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist
        """
        existing = self.design_get(name, use_devmode=True)
        rv = self.design_create(name, existing.value, use_devmode=False,
                                syncwait=syncwait)
        self.design_delete(name, use_devmode=True,
                           syncwait=syncwait)
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

        """
        name = self._mk_devmode(name, use_devmode)
        existing = None
        if syncwait:
            try:
                existing = self.design_get(name, use_devmode=False)
            except CouchbaseError:
                pass

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                  path="_design/"+name,
                                  method=_LCB.LCB_HTTP_METHOD_DELETE,
                                  fetch_headers=True)

        self._design_poll(name, 'del', existing, syncwait)
        return ret

    def __repr__(self):
        return ("<{modname}.{cls} bucket={bucket}, "
                "nodes={nodes} at 0x{oid:x}>").format(
            modname = __name__,
            cls = self.__class__.__name__,
            nodes = self.server_nodes,
            bucket = self.bucket,
            oid = id(self))
