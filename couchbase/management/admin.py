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

import couchbase_core._libcouchbase as LCB

from couchbase_core.connstr import ConnectionString
from couchbase_core import JSON, mk_formstr
import couchbase.exceptions as E
from couchbase_core._pyport import basestring
from couchbase.auth import AuthDomain
from couchbase_core._libcouchbase import FMT_JSON
from couchbase_core.supportability import internal

import re

METHMAP = {
    'GET': LCB.LCB_HTTP_METHOD_GET,
    'PUT': LCB.LCB_HTTP_METHOD_PUT,
    'POST': LCB.LCB_HTTP_METHOD_POST,
    'DELETE': LCB.LCB_HTTP_METHOD_DELETE
}


class NotReadyException(E.CouchbaseException):
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
    :class:`~couchbase_core.client.Client` is used for that.
    """
    @internal
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
            :exc:`couchbase.exceptions.AuthenticationException` if incorrect credentials
            were supplied

            :exc:`couchbase.exceptions.ConnectException` if there was a problem
            establishing a connection to the provided host

        :return: an instance of :class:`Admin`
        """
        self.__is_6_5 = None
        connection_string = kwargs.pop('connection_string', None)

        if not connection_string:
            connection_string = ConnectionString(hosts=["{0}:{1}".format(host, port)],
                                                 scheme=kwargs.get('scheme', 'http'), bucket=kwargs.get('bucket', None))
            ipv6 = kwargs.pop('ipv6', None)
            if ipv6:
                connection_string.set_option('ipv6', ipv6)

        kwargs.update({
            'username': username,
            'password': password,
            'connection_string': str(connection_string),
            '_conntype': LCB.LCB_TYPE_CLUSTER
        })
        super(Admin, self).__init__(**kwargs)
        self._connect()

    def _is_6_5_plus(self):

        # lets just check once.  Below, we will only set this if we are sure about the value.
        if self.__is_6_5 is not None:
            return self.__is_6_5

        try:
            response = self.http_request(path="/pools").value
            v = response.get("implementationVersion")
            # lets just get first 3 characters -- the string should be X.Y.Z-XXXX-YYYY and we only care about
            # major and minor version
            self.__is_6_5 = (float(v[:3]) >= 6.5)
        except E.NetworkException as e:
            # the cloud doesn't let us query this endpoint, and so lets assume this is a cloud instance.  However
            # lets not actually set the __is_6_5 flag as this also could be a transient error.  That means cloud
            # instances check every time, but this is only temporary.
            return True
        except ValueError:
            # this comes from the conversion to float -- the mock says "CouchbaseMock..."
            self.__is_6_5 = True
        return self.__is_6_5

    @internal
    def http_request(self,
                     path,
                     method='GET',
                     content=None,
                     content_type="application/json",
                     response_format=FMT_JSON,
                     timeout=None):
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
          :const:`~couchbase_core.FMT_JSON`.

          Note that if the conversion fails, the content will be returned as
          ``bytes``

        :raise:
          :exc:`~.InvalidArgumentException`
            if the method supplied was incorrect.
          :exc:`~.ConnectException`
            if there was a problem establishing a connection.
          :exc:`~.HTTPException`
            if the server responded with a negative reply

        :return: a :class:`~.HttpResult` object.

        .. seealso:: :meth:`bucket_create`, :meth:`bucket_remove`
        """
        imeth = None
        if not method in METHMAP:
            raise E.InvalidArgumentException.pyexc("Unknown HTTP Method", method)

        imeth = METHMAP[method]
        return self._http_request(type=LCB.LCB_HTTP_TYPE_MANAGEMENT,
                                  path=path,
                                  method=imeth,
                                  content_type=content_type,
                                  post_data=content,
                                  response_format=response_format,
                                  timeout=timeout)

    bc_defaults=dict(bucket_type='couchbase',
                  bucket_password='', replicas=0,
                  flush_enabled=False)

    @internal
    def bucket_create(self, name, **kwargs):
        """
        Create a new bucket

        :param string name: The name of the bucket to create
        :param string bucket_type: The type of bucket to create. This
            can either be `couchbase` to create a couchbase_core style
            bucket (which persists data and supports replication) or
            `memcached` (which is memory-only and does not support
            replication).
            Since Couchbase version 5.0, you can also specify
            `ephemeral`, which is a replicated bucket which does
            not have strict disk persistence requirements
        :param string bucket_password: The bucket password. This can be
            empty to disable authentication. This can be changed later on
            using :meth:`update_bucket`
        :param int replicas: The number of replicas to use for this
            bucket. The maximum number of replicas is currently 3.
            This setting can be changed via :meth:`update_bucket`
        :param int ram_quota:
            The maximum amount of memory (per node) that this bucket
            may use, in megabytes. The minimum for this value is 100.
            This setting may be changed via :meth:`update_bucket`.
        :param bool flush_enabled:
            Whether the flush API is enabled. When the flush API is
            enabled, any client connected to the bucket is able to
            clear its contents. This may be useful in development but
            not recommended in production. This setting may be changed
            via :meth:`update_bucket`
        :return: A :class:`~.HttpResult`
        :raise: :exc:`~.HTTPException` if the bucket could not be created.
        """
        final_opts = dict(**Admin.bc_defaults)
        final_opts.update(**{k: v for k, v in kwargs.items() if (v is not None)})
        params = {
            'name': name,
            'bucketType': final_opts['bucket_type'],
            'authType': 'sasl',
            'saslPassword': final_opts['bucket_password'],
            'flushEnabled': int(final_opts['flush_enabled']),
            'ramQuotaMB': final_opts['ram_quota']
        }
        if final_opts['bucket_type'] in ('couchbase', 'membase', 'ephemeral'):
            params['replicaNumber'] = final_opts['replicas']

        return self.http_request(
            path='/pools/default/buckets', method='POST',
            content=mk_formstr(params),
            content_type='application/x-www-form-urlencoded')

    @internal
    def bucket_remove(self, name):
        """
        Remove an existing bucket from the cluster

        :param string name: The name of the bucket to remove
        :return: A :class:`~.HttpResult`
        :raise: :exc:`~HTTPException` on error
        """
        return self.http_request(path='/pools/default/buckets/' + name,
                                 method='DELETE')

    bucket_delete = bucket_remove

    @internal
    class BucketInfo(object):
        """
        Information about a bucket
        """
        def __init__(self,
                     raw_json  # type: JSON
                     ):
            self.raw_json = raw_json

        def name(self):
            """
            Name of the bucket.
            :return: A :class:`str` containing the bucket name.
            """
            return self.raw_json.get("name")

        def __str__(self):
            return "Bucket named {}".format(self.name)

    @internal
    def buckets_list(self):
        """
        Retrieve the list of buckets from the server
        :return: An iterable of :Class:`Admin.BucketInfo` objects describing
        the buckets currently active on the cluster.
        """
        buckets_list = self.http_request(path='/pools/default/buckets', method='GET')
        return map(Admin.BucketInfo, buckets_list.value)

    @internal
    def bucket_info(self, name):
        """
        Retrieve information about the bucket.

        :param string name: The name of the bucket
        :return: A :class:`~.HttpResult` object. The result's
            :attr:`~.HttpResult.value` attribute contains
            A dictionary containing the bucket's information.
            The returned object is considered to be opaque, and is
            intended primarily for use with :meth:`update_bucket`.
            Currently this returns the raw decoded JSON as emitted
            by the corresponding server-side API
        :raise: :exc:`~.HTTPException` if the request failed
        """
        return self.http_request(path='/pools/default/buckets/' + name)

    @internal
    def wait_ready(self, name, timeout=5.0, sleep_interval=0.2):
        """
        Wait for a newly created bucket to be ready.

        :param string name: the name to wait for
        :param seconds timeout: the maximum amount of time to wait
        :param seconds sleep_interval: the number of time to sleep
            between each probe
        :raise: :exc:`.CouchbaseException` on internal HTTP error
        :raise: :exc:`NotReadyException` if all nodes could not be
            ready in time
        """
        end = time() + timeout
        while True:
            try:
                info = self.bucket_info(name).value
                for node in info['nodes']:
                    if node['status'] != 'healthy':
                        raise NotReadyException.pyexc('Not all nodes are healthy')
                return  # No error and all OK
            except E.CouchbaseException:
                if time() + sleep_interval > end:
                    raise
                sleep(sleep_interval)

    @staticmethod
    def _get_management_path(auth_domain, userid=None):

        if isinstance(auth_domain, str) and auth_domain in ["local", "external"]:
            domain = auth_domain
        elif isinstance(auth_domain, AuthDomain):
            domain = AuthDomain.to_str(auth_domain)
        else:
            raise E.InvalidArgumentException.pyexc("Unknown Authentication Domain", auth_domain)

        path = '/settings/rbac/users/{0}'.format(domain)
        if userid is not None:
            path = '{0}/{1}'.format(path, userid)

        return path

    @internal
    def user_get(self, username, domain, timeout=None):
        """
        Retrieve a user from the server

        :param username: The user ID.
        :param AuthDomain domain: The authentication domain for the user.

        :raise: :exc:`couchbase.exceptions.HTTPException` if the user does not exist.

        :return: :class:`~.HttpResult`. The user can be obtained from the
            returned object's `value` property.
        """
        path = self._get_management_path(domain, username)
        return self.http_request(path=path,
                                 method='GET',
                                 timeout=timeout)

    @internal
    def users_get(self, domain, timeout=None):
        """
        Retrieve a list of users from the server.

        :param AuthDomain domain: The authentication domain to retrieve users from.
        :return: :class:`~.HttpResult`. The list of users can be obtained from
            the returned object's `value` property.
        """
        path = self._get_management_path(domain)
        return self.http_request(path=path,
                                 method='GET',
                                 timeout=timeout)

    @internal
    def user_upsert(self, username, domain, password=None, roles=None, groups=None, name=None, timeout=None):
        """
        Upsert a user in the cluster

        :param username: The new username or user to update
        :param AuthDomain domain: The authentication domain for the user.
        :param password: The user password
        :param roles: A list of roles. A role can either be a simple string,
            or a list of `(role, bucket)` pairs.
        :param name: role display name
        :param timeout: time allowed for operation to be terminated.
            This is controlled by the client.
        :raise: :exc:`couchbase.exceptions.HTTPException` if the request fails.
        :return: :class:`~.HttpResult`

        Creating a new read-only admin user ::

            adm.user_upsert(AuthDomain.Local, 'mark', 's3cr3t', ['ro_admin'])

        An example of using more complex roles ::

            adm.user_upsert(AuthDomain.Local, 'mark', 's3cr3t',
                                              [('data_reader', '*'),
                                               ('data_writer', 'inbox')])


        .. warning::

           Due to the asynchronous nature of Couchbase management APIs, it may
           take a few moments for the new user settings to take effect.
        """
        if not groups and (not roles or not isinstance(roles, list)):
            raise E.InvalidArgumentException("Roles must be a non-empty list")

        if isinstance(domain, AuthDomain):
            domain = AuthDomain.to_str(domain)

        if password and domain == "external":
            raise E.InvalidArgumentException("External domains must not have passwords")
        
        params = {}
        if roles:
            params['roles'] = ','.join(list(map(lambda r: r.to_server_str(), roles)))
        # For backwards compatibility with Couchbase Server 6.0 and earlier,
        # the "groups" parameter MUST be omitted if the group list is empty. 
        # Couchbase Server 6.5 treats the absent parameter the same as an 
        # explicit parameter with no value (removes any existing group associations, 
        # which is what we want in this case).
        if groups and self._is_6_5_plus():
            params['groups'] = ','.join(groups)
        if password:
            params['password'] = password
        if name:
            params['name'] = name

        form = mk_formstr(params)
        path = self._get_management_path(domain, username)
        return self.http_request(path=path,
                                 method='PUT',
                                 content_type='application/x-www-form-urlencoded',
                                 content=form,
                                 timeout=timeout)

    @internal
    def user_remove(self, username, domain, timeout=None):
        """
        Remove a user
        
        :param AuthDomain domain: The authentication domain for the user.
        :param username: The user ID to remove

        :raise: :exc:`couchbase.exceptions.HTTPException` if the user does not exist.

        :return: :class:`~.HttpResult`
        """
        path = self._get_management_path(domain, username)
        return self.http_request(path=path,
                                 method='DELETE',
                                 timeout=timeout)

    @internal
    def get_roles(self, timeout=None):
        """
        Retrieve roles from the server

        :param timeout: time allowed for operation to be terminated.
            This is controlled by the client.

        :raise: :exc:`couchbase.exceptions.HTTPException` if the request fails.

        :return: :class:`~.HttpResult`. The user can be obtained from the
            returned object's `value` property.
        """
        return self.http_request(path="/settings/rbac/roles/",
                                 method='GET',
                                 timeout=timeout)
    
    @internal
    def group_upsert(self, group_name, roles=None, description=None, ldap_group_reference=None, timeout=None):
        """
        Upsert a group in the cluster

        :param group_name: The name of the group
        :param roles: A list of roles
        :param description: The description of the group
        :param ldap_group_reference: The external LDAP group reference
        :param timeout: time allowed for operation to be terminated.
            This is controlled by the client.

        :raise: :exc:`couchbase.exceptions.HTTPException` if the request fails.

        :return: :class:`~.HttpResult`

        .. warning::

           Due to the asynchronous nature of Couchbase management APIs, it may
           take a few moments for the new user settings to take effect.
        """
        params = {}
        if roles:
            params['roles'] = ','.join(list(map(lambda r: r.to_server_str(), roles)))

        if description:
            params['description'] = description

        if ldap_group_reference:
            params['ldap_group_ref'] = ldap_group_reference

        path = "/settings/rbac/groups/{}".format(group_name)
        form = mk_formstr(params)
        return self.http_request(path=path,
                                 method='PUT',
                                 content_type='application/x-www-form-urlencoded',
                                 content=form,
                                 timeout=timeout)

    @internal
    def group_get(self, group_name, timeout=None):
        """
        Retrieve a group from the server

        :param group_name: the name of the group to get
        
        :raise: :exc:`couchbase.exceptions.HTTPException` if the group does not exist.

        :return: :class:`~.HttpResult`. The group can be obtained from the
            returned object's `value` property.
        """

        path = '/settings/rbac/groups/{}'.format(group_name)
        return self.http_request(path=path,
                                 method='GET',
                                 timeout=timeout)

    @internal
    def groups_get(self, timeout=None):
        """
        Retrieve a list of groups from the server.

        :raise: :exc:`couchbase.exceptions.HTTPException` if the request fails.

        :return: :class:`~.HttpResult`. The list of users can be obtained from
            the returned object's `value` property.
        """
        
        return self.http_request(path='/settings/rbac/groups/',
                                 method='GET',
                                 timeout=timeout)

    @internal
    def group_remove(self, group_name, **kwargs):
        """
        Remove a group
        
        :param group_name: the name of the group to get

        :raise: :exc:`couchbase.exceptions.HTTPException` if the group does not exist.

        :return: :class:`~.HttpResult`
        """
        path = '/settings/rbac/groups/{}'.format(group_name)
        return self.http_request(path=path,
                                 method='DELETE',
                                 **kwargs)