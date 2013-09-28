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
from collections import deque

import couchbase.connection
import couchbase._libcouchbase as LCB
import couchbase.exceptions as E
from couchbase.user_constants import FMT_JSON


METHMAP = {
    'GET': LCB.LCB_HTTP_METHOD_GET,
    'PUT': LCB.LCB_HTTP_METHOD_PUT,
    'POST': LCB.LCB_HTTP_METHOD_POST,
    'DELETE': LCB.LCB_HTTP_METHOD_DELETE
}


class Admin(LCB.Connection):
    """An administrative connection to a Couchbase cluster.

    With this object, you can do things which affect the cluster, such as
    modifying buckets, allocating nodes, or retrieving information about
    the cluster.

    This object should **not** be used to perform Key/Value operations. The
    :class:`couchbase.connection.Connection` is used for that.
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
        kwargs = {
            'username': username,
            'password': password,
            'host': "{0}:{1}".format(host, port),
            '_conntype': LCB.LCB_TYPE_CLUSTER,
            '_errors': deque()
        }

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

        See <LINK?> for a list of available comments.

        Note that this is a fairly low level function. This class will with
        time contain more and more wrapper methods for common tasks such
        as bucket creation or node allocation, and this method should
        mostly be used if a wrapper is not available.

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
          :attr:`~couchbase.result.HttpResult.value` field of the
          :class:`~couchbase.result.HttpResult` object. The default is
          :const:`~couchbase.connection.FMT_JSON`.

          Note that if the conversion fails, the content will be returned as
          ``bytes``

        :raise:

          :exc:`couchbase.exceptions.ArgumentError` if the method supplied was
            incorrect

          :exc:`couchbase.exceptions.ConnectError` if there was a problem
            establishing a connection.

          :exc:`couchbase.exceptions.HTTPError` if the server responded with a
            negative reply

        :return: a :class:`~couchbase.result.HttpResult` object.
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
