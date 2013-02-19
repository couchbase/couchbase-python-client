#
# Copyright 2012, Couchbase, Inc.
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


class CouchbaseHttpExceptionTypes(object):

    UNAUTHORIZED = 1000
    NOT_REACHABLE = 1001
    NODE_ALREADY_JOINED = 1002
    NODE_CANT_ADD_TO_ITSELF = 1003
    BUCKET_CREATION_ERROR = 1004
    STATS_UNAVAILABLE = 1005
    BUCKET_UNAVAILABLE = 1006


class MemcachedError(Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg = 'Memcached error #' + repr(status)
        if msg:
            supermsg += ":  " + msg
        Exception.__init__(self, supermsg)

        self.status = status
        self.msg = msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)


class MemcachedTimeoutException(Exception):
    def __init__(self, item, timeout):
        msg = ("timeout - memcached did not return in %s second during %s"
               " operation for key %s")
        self._message = msg % (timeout, item["operation"], item["key"])

    def __str__(self):
        string = ''
        if self._message:
            string += self._message
        return string


class MemcachedConfigurationError(MemcachedError):
    """MemcachedError "upgrading" for any error that can be "fixed" by a
    configuration change"""
    def __init__(self, status, msg):
        super(MemcachedError, self)
        if self.status is 129:
            self.msg = 'SASL Authentication is not enabled for this' \
                       ' memcached server.'


#base exception class for couchbase apis
class CouchbaseHttpException(Exception):
    def __init__(self, message='', errorType='', parameters=dict()):
        self._message = message
        self.type = errorType
        #you can embed the params values here
        #dictionary mostly
        self.parameters = parameters

    def __str__(self):
        string = ''
        if self._message:
            string += self._message
        return string


class UnauthorizedException(CouchbaseHttpException):
    def __init__(self, username='', password=''):
        self._message = 'user not logged in'
        self.parameters = dict()
        self.parameters['username'] = username
        self.parameters['password'] = password
        self.type = CouchbaseHttpExceptionTypes.UNAUTHORIZED


class BucketCreationException(CouchbaseHttpException):
    def __init__(self, ip='', bucket_name='', error=''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.parameters['bucket'] = bucket_name
        self.type = CouchbaseHttpExceptionTypes.BUCKET_CREATION_ERROR
        self._message = ('unable to create bucket %s on the host @ %s' %
                         (bucket_name, ip))
        if error:
            self._message += ' due to error: ' + error


class BucketUnavailableException(CouchbaseHttpException):
    def __init__(self, ip='', bucket_name='', error=''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.parameters['bucket'] = bucket_name
        self.type = CouchbaseHttpExceptionTypes.BUCKET_UNAVAILABLE
        self._message = ('unable to find bucket %s on the host @ %s' %
                         (bucket_name, ip))
        if error:
            self._message += ' due to error: ' + error


class StatsUnavailableException(CouchbaseHttpException):
    def __init__(self):
        self.type = CouchbaseHttpExceptionTypes.STATS_UNAVAILABLE
        self._message = 'unable to get stats'


class ServerUnavailableException(CouchbaseHttpException):
    def __init__(self, ip=''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.type = CouchbaseHttpExceptionTypes.NOT_REACHABLE
        self._message = 'unable to reach the host @ %s' % (ip)


class InvalidArgumentException(CouchbaseHttpException):
    def __init__(self, api, parameters):
        self.parameters = parameters
        self.api = api
        self._message = ('%s failed when invoked with parameters: %s' %
                         (self.api, self.parameters))


class ServerJoinException(CouchbaseHttpException):
    def __init__(self, nodeIp='', remoteIp=''):
        self._message = ('node: %s already added to this cluster:%s' %
                         (remoteIp, nodeIp))
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = CouchbaseHttpExceptionTypes.NODE_CANT_ADD_TO_ITSELF


class ServerAlreadyJoinedException(CouchbaseHttpException):
    def __init__(self, nodeIp='', remoteIp=''):
        self._message = ('node: %s already added to this cluster:%s' %
                         (remoteIp, nodeIp))
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = CouchbaseHttpExceptionTypes.NODE_ALREADY_JOINED
