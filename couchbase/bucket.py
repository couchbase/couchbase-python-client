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
from warnings import warn

import couchbase._bootstrap
import couchbase._libcouchbase as _LCB
from couchbase._libcouchbase import Bucket as _Base

from couchbase.exceptions import *
from couchbase.user_constants import *
from couchbase.result import *
from couchbase.bucketmanager import BucketManager

import couchbase.exceptions as exceptions
from couchbase.views.params import make_dvpath, make_options_string
from couchbase.views.iterator import View
from couchbase.n1ql import N1QLQuery, N1QLRequest
import couchbase.fulltext as _FTS
from couchbase._pyport import basestring


def _depr(fn, usage, stacklevel=3):
    """Internal convenience function for deprecation warnings"""
    warn('{0} is deprecated. Use {1} instead'.format(fn, usage),
         stacklevel=stacklevel, category=DeprecationWarning)


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


class Bucket(_Base):
    def __init__(self, *args, **kwargs):
        """Connect to a bucket.

        :param string connection_string:
            The connection string to use for connecting to the bucket.
            This is a URI-like string allowing specifying multiple hosts
            and a bucket name.

            The format of the connection string is the *scheme*
            (``couchbase`` for normal connections, ``couchbases`` for
            SSL enabled connections); a list of one or more *hostnames*
            delimited by commas; a *bucket* and a set of options.

            like so::

                couchbase://host1,host2,host3/bucketname?option1=value1&option2=value2

            If using the SSL scheme (``couchbases``), ensure to specify
            the ``certpath`` option to point to the location of the
            certificate on the client's filesystem; otherwise connection
            may fail with an error code indicating the server's
            certificate could not be trusted.

            See :ref:`connopts` for additional connection options.

        :param string password: the password of the bucket

        :param boolean quiet: the flag controlling whether to raise an
            exception when the client executes operations on
            non-existent keys. If it is `False` it will raise
            :exc:`.NotFoundError` exceptions. When
            set to `True` the operations will return `None` silently.

        :param boolean unlock_gil: If set (which is the default), the
            bucket object will release the python GIL when possible,
            allowing other (Python) threads to function in the
            background. This should be set to true if you are using
            threads in your application (and is the default), as
            otherwise all threads will be blocked while couchbase
            functions execute.

            You may turn this off for some performance boost and you are
            certain your application is not using threads

        :param transcoder:
            Set the transcoder object to use. This should conform to the
            interface in the documentation (it need not actually be a
            subclass). This can be either a class type to instantiate,
            or an initialized instance.
        :type transcoder: :class:`.Transcoder`

        :param lockmode: The *lockmode* for threaded access.
            See :ref:`multiple_threads` for more information.

        :raise: :exc:`.BucketNotFoundError` or :exc:`.AuthError` if
            there is no such bucket to connect to, or if invalid
            credentials were supplied.
        :raise: :exc:`.CouchbaseNetworkError` if the socket wasn't
            accessible (doesn't accept connections or doesn't respond
            in
        :raise: :exc:`.InvalidError` if the connection string
            was malformed.

        :return: instance of :class:`~.Bucket`


        Initialize bucket using default options::

            from couchbase.bucket import Bucket
            cb = Bucket('couchbase:///mybucket')

        Connect to protected bucket::

            cb = Bucket('couchbase:///protected', password='secret')

        Connect using a list of servers::

            cb = Bucket('couchbase://host1,host2,host3/mybucket')

        Connect using SSL::

            cb = Bucket('couchbases://securehost/bucketname?certpath=/var/cb-cert.pem')

        """
        _no_connect_exceptions = kwargs.pop('_no_connect_exceptions', False)
        _cntlopts = kwargs.pop('_cntl', {})

        # The following two blocks adapt some options from 1.x to proper
        # connection string (or lcb_cntl_string()) settings.
        strcntls = {}
        if 'timeout' in kwargs:
            _depr('timeout keyword argument',
                  'operation_timeout (with float value) in connection string')
            strcntls['operation_timeout'] = str(float(kwargs.pop('timeout')))

        if 'config_cache' in kwargs:
            _depr('config_cache keyword argument',
                  'config_cache in connection string')
            strcntls['config_cache'] = kwargs.pop('config_cache')

        tc = kwargs.get('transcoder')
        if isinstance(tc, type):
            kwargs['transcoder'] = tc()

        super(Bucket, self).__init__(*args, **kwargs)
        # Enable detailed error codes for network errors:
        self._cntlstr("detailed_errcodes", "1")

        # Enable self-identification in logs
        try:
            from couchbase._version import __version__ as cb_version
            self._cntlstr('client_string', 'PYCBC/' + cb_version)
        except E.NotSupportedError:
            pass

        for ctl, val in strcntls.items():
            self._cntlstr(ctl, val)

        for ctl, val in _cntlopts.items():
            self._cntl(ctl, val)

        try:
            self._do_ctor_connect()
        except exceptions.CouchbaseError as e:
            if not _no_connect_exceptions:
                raise

    def _do_ctor_connect(self):
        """This should be overidden by subclasses which want to use a
        different sort of connection behavior
        """
        self._connect()

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
        """Unconditionally store the object in Couchbase.

        :param key:
            The key to set the value with. By default, the key must be
            either a :class:`bytes` or :class:`str` object encodable as
            UTF-8. If a custom `transcoder` class is used (see
            :meth:`~__init__`), then the key object is passed directly
            to the transcoder, which may serialize it how it wishes.
        :type key: string or bytes

        :param value: The value to set for the key. The type for `value`
            follows the same rules as for `key`

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

            cb.upsert('foo', {'bar': 'baz'}, format=couchbase.FMT_JSON)

        Perform optimistic locking by specifying last known CAS version::

            cb.upsert('foo', 'bar', cas=8835713818674332672)

        Several sets at the same time (mutli-set)::

            cb.upsert_multi({'foo': 'bar', 'baz': 'value'})

        .. seealso:: :meth:`upsert_multi`
        """
        return _Base.upsert(self, key, value, cas=cas, ttl=ttl,
                            format=format, persist_to=persist_to,
                            replicate_to=replicate_to)

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
        return _Base.insert(self, key, value, ttl=ttl, format=format,
                            persist_to=persist_to, replicate_to=replicate_to)

    def replace(self, key, value, cas=0, ttl=0, format=None,
                persist_to=0, replicate_to=0):
        """Store an object in Couchbase only if it already exists.

        Follows the same conventions as :meth:`upsert`, but the value is
        stored only if a previous value already exists.

        :raise: :exc:`.NotFoundError` if the key does not exist

        .. seealso:: :meth:`upsert`, :meth:`replace_multi`
        """
        return _Base.replace(self, key, value, ttl=ttl, cas=cas, format=format,
                             persist_to=persist_to, replicate_to=replicate_to)

    def append(self, key, value, cas=0, format=None,
               persist_to=0, replicate_to=0):
        """Append a string to an existing value in Couchbase.

        :param string value: The data to append to the existing value.

        Other parameters follow the same conventions as :meth:`upsert`.

        The `format` argument must be one of
        :const:`~couchbase.FMT_UTF8` or :const:`~couchbase.FMT_BYTES`.
        If not specified, it will be :const:`~.FMT_UTF8` (overriding the
        :attr:`default_format` attribute). This is because JSON or
        Pickle formats will be nonsensical when random data is appended
        to them. If you wish to modify a JSON or Pickle encoded object,
        you will need to retrieve it (via :meth:`get`), modify it, and
        then store it again (using :meth:`upsert`).

        Additionally, you must ensure the value (and flags) for the
        current value is compatible with the data to be appended. For an
        example, you may append a :const:`~.FMT_BYTES` value to an
        existing :const:`~couchbase.FMT_JSON` value, but an error will
        be thrown when retrieving the value using :meth:`get` (you may
        still use the :attr:`data_passthrough` to overcome this).

        :raise: :exc:`.NotStoredError` if the key does not exist

        .. seealso:: :meth:`upsert`, :meth:`append_multi`
        """
        return _Base.append(self, key, value, cas=cas, format=format,
                            persist_to=persist_to, replicate_to=replicate_to)

    def prepend(self, key, value, cas=0, format=None,
                persist_to=0, replicate_to=0):
        """Prepend a string to an existing value in Couchbase.

        .. seealso:: :meth:`append`, :meth:`prepend_multi`
        """
        return _Base.prepend(self, key, value, cas=cas, format=format,
                             persist_to=persist_to, replicate_to=replicate_to)

    def get(self, key, ttl=0, quiet=None, replica=False, no_format=False):
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
            `False`) it will override the `Bucket`-level
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
            always be delivered in the :class:`~couchbase.result.Result`
            object as being of :data:`~couchbase.FMT_BYTES`. This is a
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

        .. seealso::
            :meth:`get` - which can be used to get *and* update the
                expiration,
            :meth:`touch_multi`
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
        using the default :const:`couchbase.FMT_JSON` then the value
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

    def mutate_in(self, key, *specs, **kwargs):
        """Perform multiple atomic modifications within a document.

        :param key: The key of the document to modify
        :param specs: A list of specs (See :mod:`.couchbase.subdocument`)
        :param kwargs: CAS, etc.
        :return: A :class:`~.couchbase.result.SubdocResult` object.

        Here's an example of adding a new tag to a "user" document
        and incrementing a modification counter::

            import couchbase.subdocument as SD
            # ....
            cb.mutate_in('user',
                         SD.array_addunique('tags', 'dog'),
                         SD.counter('updates', 1))

        .. seealso:: :mod:`.couchbase.subdocument`
        """
        return super(Bucket, self).mutate_in(key, specs, **kwargs)

    def lookup_in(self, key, *specs, **kwargs):
        """Atomically retrieve one or more paths from a document.

        :param key: The key of the document to lookup
        :param spec: A list of specs (see :mod:`.couchbase.subdocument`)
        :return: A :class:`.couchbase.result.SubdocResult` object.
            This object contains the results and any errors of the
            operation.

        Example::

            import couchbase.subdocument as SD
            rv = cb.lookup_in('user',
                              SD.get('email'),
                              SD.get('name'),
                              SD.exists('friends.therock'))

            email = rv[0]
            name = rv[1]
            friend_exists = rv.exists(2)

        .. seealso:: :meth:`retrieve_in` which acts as a convenience wrapper
        """
        return super(Bucket, self).lookup_in({key: specs}, **kwargs)

    def retrieve_in(self, key, *paths, **kwargs):
        """Atomically fetch one or more paths from a document.

        Convenience method for retrieval operations. This functions
        identically to :meth:`lookup_in`. As such, the following two
        forms are equivalent:

        .. code-block:: python

            import couchbase.subdocument as SD
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
        import couchbase.subdocument as SD
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
            on the returned :class:`~couchbase.result.MultiResult` than
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
        :return: a :class:`~couchbase.result.MultiResult` object

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
            * A :class:`~couchbase.result.MultiResult` object
            * A ``dict`` with CAS values as the dictionary value
            * A sequence of :class:`~couchbase.result.Result` objects

        :return: A :class:`~.MultiResult` object
            of :class:`~.OperationResult` items.

        .. seealso:: :meth:`endure`
        """
        return _Base.endure_multi(self, keys, persist_to=persist_to,
                                  replicate_to=replicate_to,
                                  timeout=timeout, interval=interval,
                                  check_removed=check_removed)

    def remove_multi(self, kvs, quiet=None):
        """Remove multiple items from the cluster

        :param kvs: Iterable of keys to delete from the cluster. If you wish
            to specify a CAS for each item, then you may pass a dictionary
            of keys mapping to cas, like ``remove_multi({k1:cas1, k2:cas2})
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

    def rget(self, key, replica_index=None, quiet=None):
        """Get an item from a replica node

        :param string key: The key to fetch
        :param int replica_index: The replica index to fetch.
            If this is ``None`` then this method will return once any
            replica responds. Use :attr:`configured_replica_count` to
            figure out the upper bound for this parameter.

            The value for this parameter must be a number between 0 and
            the value of :attr:`configured_replica_count`-1.
        :param boolean quiet: Whether to suppress errors when the key is
            not found

        This method (if `replica_index` is not supplied) functions like
        the :meth:`get` method that has been passed the `replica`
        parameter::

            c.get(key, replica=True)

        .. seealso:: :meth:`get` :meth:`rget_multi`
        """
        if replica_index is not None:
            return _Base._rgetix(self, key, replica=replica_index, quiet=quiet)
        else:
            return _Base._rget(self, key, quiet=quiet)

    def rget_multi(self, keys, replica_index=None, quiet=None):
        if replica_index is not None:
            return _Base._rgetix_multi(self, keys,
                                       replica=replica_index, quiet=quiet)
        else:
            return _Base._rget_multi(self, keys, quiet=quiet)

    def _view(self, ddoc, view,
              use_devmode=False,
              params=None,
              unrecognized_ok=False,
              passthrough=False):
        """Internal method to Execute a view (MapReduce) query

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

    @staticmethod
    def _mk_devmode(n, use_devmode):
        if n.startswith('dev_') or not use_devmode:
            return n
        return 'dev_' + n

    def bucket_manager(self):
        """
        Returns a :class:`~.BucketManager` object which may be used to
        perform management operations on the current bucket. These
        operations may create/modify design documents and flush the
        bucket
        """
        return BucketManager(self)

    def query(self, design, view, use_devmode=False, **kwargs):
        """
        Query a pre-defined MapReduce view, passing parameters.

        This method executes a view on the cluster. It accepts various
        parameters for the view and returns an iterable object
        (specifically, a :class:`~.View`).

        :param string design: The design document
        :param string view: The view function contained within the design
            document
        :param boolean use_devmode: Whether the view name should be
            transformed into a development-mode view. See documentation
            on :meth:`~.BucketManager.design_create` for more
            explanation.
        :param kwargs: Extra arguments passed to the :class:`~.View`
            object constructor.
        :param kwargs: Additional parameters passed to the
            :class:`~.View` constructor. See that class'
            documentation for accepted parameters.

        .. seealso::

            :class:`~.View`
                contains more extensive documentation and examples

            :class:`~.Query`
                contains documentation on the available query options

            :class:`~.SpatialQuery`
                contains documentation on the available query options
                for Geospatial views.

        .. note::

            To query a spatial view, you must explicitly use the
            :class:`.SpatialQuery`. Passing key-value view parameters
            in ``kwargs`` is not supported for spatial views.

        """
        design = self._mk_devmode(design, use_devmode)
        itercls = kwargs.pop('itercls', View)
        return itercls(self, design, view, **kwargs)

    def n1ql_query(self, query, *args, **kwargs):
        """
        Execute a N1QL query.

        This method is mainly a wrapper around the :class:`~.N1QLQuery`
        and :class:`~.N1QLRequest` objects, which contain the inputs
        and outputs of the query.

        Using an explicit :class:`~.N1QLQuery`::

            query = N1QLQuery(
                'SELECT airportname FROM `travel-sample` WHERE city=$1', "Reno")
            # Use this option for often-repeated queries
            query.adhoc = False
            for row in cb.n1ql_query(query):
                print 'Name: {0}'.format(row['airportname'])

        Using an implicit :class:`~.N1QLQuery`::

            for row in cb.n1ql_query(
                'SELECT airportname, FROM `travel-sample` WHERE city=$1', "Reno"):
                print 'Name: {0}'.format(row['airportname'])

        :param query: The query to execute. This may either be a
            :class:`.N1QLQuery` object, or a string (which will be
            implicitly converted to one).
        :param kwargs: Arguments for :class:`.N1QLRequest`.
        :return: An iterator which yields rows. Each row is a dictionary
            representing a single result
        """
        if not isinstance(query, N1QLQuery):
            query = N1QLQuery(query)

        itercls = kwargs.pop('itercls', N1QLRequest)
        return itercls(query, self, *args, **kwargs)

    def search(self, index, query, **kwargs):
        """
        Perform full-text searches

        .. versionadded:: 2.0.9

        .. warning::

            The full-text search API is experimental and subject to change

        :param str index: Name of the index to query
        :param couchbase.fulltext.SearchQuery query: Query to issue
        :param couchbase.fulltext.Params params: Additional query options
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

    def __repr__(self):
        return ('<{modname}.{cls} bucket={bucket}, nodes={nodes} at 0x{oid:x}>'
                ).format(modname=__name__, cls=self.__class__.__name__,
                         nodes=self.server_nodes, bucket=self.bucket,
                         oid=id(self))

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
        :type items: :class:`~couchbase.items.ItemOptionDict`.

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

    @property
    def closed(self):
        """Returns True if the object has been closed with :meth:`_close`"""
        return self._privflags & _LCB.PYCBC_CONN_F_CLOSED

    def _get_timeout_common(self, op):
        return self._cntl(op, value_type='timeout')

    def _set_timeout_common(self, op, value):
        value = float(value)
        if value <= 0:
            raise ValueError('Timeout must be greater than 0')

        self._cntl(op, value_type='timeout', value=value)

    @property
    def timeout(self):
        """
        The timeout for key-value operations, in fractions of a second.
        This timeout affects the :meth:`get` and :meth:`upsert` family
        of methods.

        ::

            # Set timeout to 3.75 seconds
            cb.timeout = 3.75

        .. seealso:: :attr:`views_timeout`, :attr:`n1ql_timeout`
        """
        return self._get_timeout_common(_LCB.LCB_CNTL_OP_TIMEOUT)

    @timeout.setter
    def timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_OP_TIMEOUT, value)

    @property
    def views_timeout(self):
        """
        The timeout for view query operations. This affects the
        :meth:`query` method.

        Timeout may be specified in fractions of a second.
        .. seealso:: :attr:`timeout`
        """
        return self._get_timeout_common(_LCB.LCB_CNTL_VIEW_TIMEOUT)

    @views_timeout.setter
    def views_timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_VIEW_TIMEOUT, value)

    @property
    def n1ql_timeout(self):
        """
        The timeout for N1QL query operations. This affects the
        :meth:`n1ql_query` method.

        Timeouts may also be adjusted on a per-query basis by setting the
        :attr:`couchbase.n1ql.N1QLQuery.timeout` property. The effective
        timeout is either the per-query timeout or the global timeout,
        whichever is lower.
        """

        return self._get_timeout_common(_LCB.LCB_CNTL_N1QL_TIMEOUT)

    @n1ql_timeout.setter
    def n1ql_timeout(self, value):
        self._set_timeout_common(_LCB.LCB_CNTL_N1QL_TIMEOUT, value)

    @property
    def is_ssl(self):
        """
        Read-only boolean property indicating whether SSL is used for
        this connection.

        If this property is true, then all communication between this
        object and the Couchbase cluster is encrypted using SSL.

        See :meth:`__init__` for more information on connection options.
        """
        mode = self._cntl(op=_LCB.LCB_CNTL_SSL_MODE, value_type='int')
        return mode & _LCB.LCB_SSL_ENABLED != 0

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

    def _cntl(self, *args, **kwargs):
        """Low-level interface to the underlying C library's settings. via
        ``lcb_cntl()``.

        This method accepts an opcode and an
        optional value. Constants are intentionally not defined for
        the various opcodes to allow saner error handling when an
        unknown opcode is not used.

        .. warning::

            If you pass the wrong parameters to this API call, your
            application may crash. For this reason, this is not a public
            API call. Nevertheless it may be used sparingly as a
            workaround for settings which may have not yet been exposed
            directly via a supported API

        :param int op: Type of cntl to access. These are defined in
          libcouchbase's ``cntl.h`` header file

        :param value: An optional value to supply for the operation.
            If a value is not passed then the operation will return the
            current value of the cntl without doing anything else.
            otherwise, it will interpret the cntl in a manner that makes
            sense. If the value is a float, it will be treated as a
            timeout value and will be multiplied by 1000000 to yield the
            microsecond equivalent for the library. If the value is a
            boolean, it is treated as a C ``int``

        :param value_type: String indicating the type of C-level value
            to be passed to ``lcb_cntl()``. The possible values are:

            * ``"string"`` - NUL-terminated `const char`.
                Pass a Python string
            * ``"int"`` - C ``int`` type. Pass a Python int
            * ``"uint32_t"`` - C ``lcb_uint32_t`` type.
                Pass a Python int
            * ``"unsigned"`` - C ``unsigned int`` type.
                Pass a Python int
            * ``"float"`` - C ``float`` type. Pass a Python float
            * ``"timeout"`` - The number of seconds as a float. This is
                converted into microseconds within the extension library.

        :return: If no `value` argument is provided, retrieves the
            current setting (per the ``value_type`` specification).
            Otherwise this function returns ``None``.
        """
        return _Base._cntl(self, *args, **kwargs)

    def _cntlstr(self, key, value):
        """
        Low-level interface to the underlying C library's settings.
        via ``lcb_cntl_string()``.

        This method accepts a key and a value. It can modify the same
        sort of settings as the :meth:`~._cntl` method, but may be a
        bit more convenient to follow in code.

        .. warning::

            See :meth:`~._cntl` for warnings.

        :param string key: The setting key
        :param string value: The setting value

        See the API documentation for libcouchbase for a list of
        acceptable setting keys.
        """
        return _Base._cntlstr(self, key, value)

    @staticmethod
    def lcb_version():
        return _LCB.lcb_version()

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

    def flush(self):
        """
        Clears the bucket's contents.

        .. note::

            This functionality requires that the flush option be
            enabled for the bucket by the cluster administrator. You
            can enable flush on the bucket using the administrative
            console (See http://docs.couchbase.com/admin/admin/UI/ui-data-buckets.html)

        .. note::

            This is a destructive operation, as it will clear all the
            data from the bucket.

        .. note::

            A successful execution of this method means that the bucket
            will have started the flush process. This does not
            necessarily mean that the bucket is actually empty.
        """
        path = '/pools/default/buckets/{0}/controller/doFlush'
        path = path.format(self.bucket)
        return self._http_request(type=_LCB.LCB_HTTP_TYPE_MANAGEMENT,
                                  path=path, method=_LCB.LCB_HTTP_METHOD_POST)

    def add_bucket_creds(self, bucket, password):
        if not bucket or not password:
            raise ValueError('Bucket and password must be nonempty')
        return _Base._add_creds(self, bucket, password)
