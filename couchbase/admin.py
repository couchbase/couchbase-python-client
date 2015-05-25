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
"""
The contents of this module do not have a stable API and are subject to
change
"""
from time import time, sleep

import couchbase._libcouchbase as LCB
import couchbase.exceptions as E
from couchbase.user_constants import FMT_JSON
from couchbase._pyport import ulp

METHMAP = {
    'GET': LCB.LCB_HTTP_METHOD_GET,
    'PUT': LCB.LCB_HTTP_METHOD_PUT,
    'POST': LCB.LCB_HTTP_METHOD_POST,
    'DELETE': LCB.LCB_HTTP_METHOD_DELETE
}


class NotReadyError(E.CouchbaseError):
    """
    Thrown when not all nodes could be ready (internal)
    """
    pass


class Admin(LCB.Bucket):
    """
    An administrative connection to a Couchbase cluster.

    With this object, you can do things which affect the cluster, such as
    modifying buckets, allocating nodes, or retrieving information about
    the cluster.

    This object should **not** be used to perform Key/Value operations. The
    :class:`~.Bucket` is used for that.
    """
    def __init__(self, username, password, host='localhost', port=8091,
                 **kwargs):

        """Connect to a cluster

        :param string username: The administrative username for the cluster,
          this is typically ``Administrator``
        :param string password: The administrative password for the cluster,
          this is the password you entered when Couchbase was installed
        :param string host: The hostname or IP of one of the nodes which is
          currently a member of the cluster (or a newly allocated node, if
          you wish to operate on that)
        :param int port: The management port for the node

        :raise:
            :exc:`couchbase.exceptions.AuthError` if incorrect credentials
            were supplied

            :exc:`couchbase.exceptions.ConnectError` if there was a problem
            establishing a connection to the provided host

        :return: an instance of :class:`Admin`
        """
        kwargs.update({
            'username': username,
            'password': password,
            'connection_string': "http://{0}:{1}".format(host, port),
            '_conntype': LCB.LCB_TYPE_CLUSTER
        })
        super(Admin, self).__init__(**kwargs)
        self._connect()

    def http_request(self,
                     path,
                     method='GET',
                     content=None,
                     content_type="application/json",
                     response_format=FMT_JSON):
        """
        Perform an administrative HTTP request. This request is sent out to
        the administrative API interface (i.e. the "Management/REST API")
        of the cluster.

        Note that this is a fairly low level function. You should use one
        of the helper methods in this class to perform your task, if
        possible.

        :param string path: The path portion (not including the host) of the
          rest call to perform. This should also include any encoded arguments.

        :param string method: This is the HTTP method to perform. Currently
          supported values are `GET`, `POST`, `PUT`, and `DELETE`

        :param bytes content: Content to be passed along in the request body.
          This is only applicable on `PUT` and `POST` methods.

        :param string content_type: Value for the HTTP ``Content-Type`` header.
          Currently this is ``application-json``, and should probably not be
          set to something else.

        :param int response_format:
          Hint about how to format the response. This goes into the
          :attr:`~.HttpResult.value` field of the
          :class:`~.HttpResult` object. The default is
          :const:`~couchbase.FMT_JSON`.

          Note that if the conversion fails, the content will be returned as
          ``bytes``

        :raise:
          :exc:`~.ArgumentError`
            if the method supplied was incorrect.
          :exc:`~.ConnectError`
            if there was a problem establishing a connection.
          :exc:`~.HTTPError`
            if the server responded with a negative reply

        :return: a :class:`~.HttpResult` object.

        .. seealso:: :meth:`bucket_create`, :meth:`bucket_remove`
        """
        imeth = None
        if not method in METHMAP:
            raise E.ArgumentError.pyexc("Unknown HTTP Method", method)

        imeth = METHMAP[method]
        return self._http_request(type=LCB.LCB_HTTP_TYPE_MANAGEMENT,
                                  path=path,
                                  method=imeth,
                                  content_type=content_type,
                                  post_data=content,
                                  response_format=response_format)

    def _mk_formstr(self, d):
        l = []
        for k, v in d.items():
            l.append('{0}={1}'.format(ulp.quote(k), ulp.quote(str(v))))

        return '&'.join(l)

    def bucket_create(self, name, bucket_type='couchbase',
                      bucket_password='', replicas=0, ram_quota=1024,
                      flush_enabled=False):
        """
        Create a new bucket

        :param string name: The name of the bucket to create
        :param string bucket_type: The type of bucket to create. This
            can either be `couchbase` to create a couchbase style
            bucket (which persists data and supports replication) or
            `memcached` (which is memory-only and does not support
            replication).
        :param string bucket_password: The bucket password. This can be
            empty to disable authentication. This can be changed later on
            using :meth:`bucket_update`
        :param int replicas: The number of replicas to use for this
            bucket. The maximum number of replicas is currently 3.
            This setting can be changed via :meth:`bucket_update`
        :param int ram_quota:
            The maximum amount of memory (per node) that this bucket
            may use, in megabytes. The minimum for this value is 100.
            This setting may be changed via :meth:`bucket_update`.
        :param bool flush_enabled:
            Whether the flush API is enabled. When the flush API is
            enabled, any client connected to the bucket is able to
            clear its contents. This may be useful in development but
            not recommended in production. This setting may be changed
            via :meth:`bucket_update`
        :return: A :class:`~.HttpResult`
        :raise: :exc:`~.HTTPError` if the bucket could not be created.
        """
        params = {
            'name': name,
            'type': bucket_type,
            'authType': 'sasl',
            'saslPassword': bucket_password if bucket_password else '',
            'flush_enabled': int(flush_enabled),
            'ramQuotaMB': ram_quota
        }
        if bucket_type in ('couchbase', 'membase'):
            params['replicaNumber'] = replicas

        return self.http_request(
            path='/pools/default/buckets', method='POST',
            content=self._mk_formstr(params),
            content_type='application/x-www-form-urlencoded')

    def bucket_remove(self, name):
        """
        Remove an existing bucket from the cluster

        :param string name: The name of the bucket to remove
        :return: A :class:`~.HttpResult`
        :raise: :exc:`~HTTPError` on error
        """
        return self.http_request(path='/pools/default/buckets/' + name,
                                 method='DELETE')

    bucket_delete = bucket_remove

    def bucket_info(self, name):
        """
        Retrieve information about the bucket.

        :param string name: The name of the bucket
        :return: A :class:`~.HttpResult` object. The result's
            :attr:`~.HttpResult.value` attribute contains
            A dictionary containing the bucket's information.
            The returned object is considered to be opaque, and is
            intended primarily for use with :meth:`bucket_update`.
            Currently this returns the raw decoded JSON as emitted
            by the corresponding server-side API
        :raise: :exc:`~.HTTPError` if the request failed
        """
        return self.http_request(path='/pools/default/buckets/' + name)

    def wait_ready(self, name, timeout=5.0, sleep_interval=0.2):
        """
        Wait for a newly created bucket to be ready.

        :param string name: the name to wait for
        :param seconds timeout: the maximum amount of time to wait
        :param seconds sleep_interval: the number of time to sleep
            between each probe
        :raise: :exc:`.CouchbaseError` on internal HTTP error
        :raise: :exc:`NotReadyError` if all nodes could not be
            ready in time
        """
        end = time() + timeout
        while True:
            try:
                info = self.bucket_info(name).value
                for node in info['nodes']:
                    if node['status'] != 'healthy':
                        raise NotReadyError.pyexc('Not all nodes are healthy')
                return  # No error and all OK
            except E.CouchbaseError:
                if time() + sleep_interval > end:
                    raise
                sleep(sleep_interval)

    def bucket_update(self, name, current, bucket_password=None, replicas=None,
                      ram_quota=None, flush_enabled=None):
        """
        Update an existing bucket's settings.

        :param string name: The name of the bucket to update
        :param dict current: Current state of the bucket.
            This can be retrieve from :meth:`bucket_info`
        :param str bucket_password: Change the bucket's password
        :param int replicas: The number of replicas for the bucket
        :param int ram_quota: The memory available to the bucket
            on each node.
        :param bool flush_enabled: Whether the flush API should be allowed
            from normal clients
        :return: A :class:`~.HttpResult` object
        :raise: :exc:`~.HTTPError` if the request could not be
            completed


        .. note::

            The default value for all options in this method is
            ``None``. If a value is set to something else, it will
            modify the setting.


        Change the bucket password::

            adm.bucket_update('a_bucket', adm.bucket_info('a_bucket'),
                              bucket_password='n3wpassw0rd')

        Enable the flush API::

            adm.bucket_update('a_bucket', adm.bucket_info('a_bucket'),
                              flush_enabled=True)
        """
        params = {}
        current = current.value

        # Merge params
        params['authType'] = current['authType']
        if 'saslPassword' in current:
            params['saslPassword'] = current['saslPassword']

        if bucket_password is not None:
            params['authType'] = 'sasl'
            params['saslPassword'] = bucket_password

        params['replicaNumber'] = (
            replicas if replicas is not None else current['replicaNumber'])

        if ram_quota:
            params['ramQuotaMB'] = ram_quota
        else:
            params['ramQuotaMB'] = current['quota']['ram'] / 1024 / 1024

        if flush_enabled is not None:
            params['flushEnabled'] = int(flush_enabled)

        params['proxyPort'] = current['proxyPort']
        return self.http_request(path='/pools/default/buckets/' + name,
                                 method='POST',
                                 content_type='application/x-www-form-urlencoded',
                                 content=self._mk_formstr(params))
