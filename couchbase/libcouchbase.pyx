from libc.stdlib cimport malloc, free
from libc.string cimport memset

cimport libcouchbase as lcb

import json
import pickle
from collections import namedtuple


cdef public enum _cb_formats:
    FMT_JSON = 0x0
    FMT_PICKLE = 0x1
    FMT_PLAIN = 0x2
    FMT_MASK = 0x3


class Const:
    store_names = {
        lcb.LCB_ADD: 'ADD',
        lcb.LCB_REPLACE: 'REPLACE',
        lcb.LCB_SET: 'SET',
        lcb.LCB_APPEND:'APPEND',
        lcb.LCB_PREPEND: 'PREPEND'
    }

class Utils:
    exc_names = {
        lcb.LCB_AUTH_ERROR: 'AuthError',
        lcb.LCB_DELTA_BADVAL: 'DeltaBadvalError',
        lcb.LCB_E2BIG: 'TooBigError',
        lcb.LCB_EBUSY: 'BusyError',
        lcb.LCB_EINTERNAL: 'InternalError',
        lcb.LCB_EINVAL: 'InvalidError',
        lcb.LCB_ENOMEM: 'NoMemoryError',
        lcb.LCB_ERANGE: 'RangeError',
        lcb.LCB_ERROR: 'LibcouchbaseError',
        lcb.LCB_ETMPFAIL: 'TemporaryFailError',
        lcb.LCB_KEY_EEXISTS: 'KeyExistsError',
        lcb.LCB_KEY_ENOENT: 'NotFoundError',
        lcb.LCB_DLOPEN_FAILED: 'DlopenFailedError',
        lcb.LCB_DLSYM_FAILED: 'DlsymFailedError',
        lcb.LCB_NETWORK_ERROR: 'NetworkError',
        lcb.LCB_NOT_MY_VBUCKET: 'NotMyVbucketError',
        lcb.LCB_NOT_STORED: 'NotStoredError',
        lcb.LCB_NOT_SUPPORTED: 'NotSupportedError',
        lcb.LCB_UNKNOWN_COMMAND: 'UnknownCommandError',
        lcb.LCB_UNKNOWN_HOST: 'UnknownHostError',
        lcb.LCB_PROTOCOL_ERROR: 'ProtocolError',
        lcb.LCB_ETIMEDOUT: 'TimeoutError',
        lcb.LCB_CONNECT_ERROR: 'ConnectError',
        lcb.LCB_BUCKET_ENOENT: 'BucketNotFoundError',
        lcb.LCB_CLIENT_ENOMEM: 'ClientNoMemoryError',
        lcb.LCB_CLIENT_ETMPFAIL: 'ClientTemporaryFailError',
        lcb.LCB_EBADHANDLE: 'BadHandleError'
    }


    @staticmethod
    def maybe_raise(rc, msg, key=None, status=0, cas=0, operation=0):
        """Raise meaningful exception

        Helper to raise a meaningful exception based on the return code
        from libcouchbase. If a success was returned, no expection will
        be raised.

        :param int rc: return code from libcouchbase
        :param string msg: the error message
        :param string key: the key that was part of the operation
        :param int status: the HTTP status code if the operation was
          through HTTP
        :param cas: the CAS value
        :param int peration: The operation that was performed on
          Couchbase (ADD, SET, REPLACE, APPEND or PREPEND)

        :raise: Any of the exceptions from :mod:`couchbase.exceptions`
        :return: no treturn value
        """
        if ((rc == lcb.LCB_SUCCESS and (status == 0 or status/100 == 2)) or
            rc == lcb.LCB_AUTH_CONTINUE):
            return

        if status > 0:
            exc_name = 'HTTPError'
        else:
            exc_name = Utils.exc_names.get(rc, 'LibcouchbaseError')

        # `exceptions` is the couchbase.exceptions module
        exception = getattr(exceptions, exc_name)(
            msg, rc, key, status, cas, operation)
        raise exception

    @staticmethod
    def raise_not_connected(operation):
        """Raise a not connected to the server error

        :param int operation: the operation that causes the error (ADD,
          SET, REPLACE, APPEND or PREPEND)
        """
        raise exceptions.CouchbaseConnectError(
            "not connected to the server",
            operation=Const.store_names[lcb.LCB_SET])


class CouchbaseError(Exception):
    # Document the init parameters here, else the output of the subclasses
    # gets too crowded
    """Base class for errors within the Couchbase Python SDK

        :param string msg: the error message
        :param int error: the error code
        :param string key: the key if it one was involved in the operation that
                           lead to the error
        :param int status: the HTTP status code if the operation was through
                           HTTP
        :param cas: the CAS value
        :param operation: The operation that was performed on Couchbase (ADD,
          SET, REPLACE, APPEND or PREPEND)
    """
    http_status_msg = {
        lcb.LCB_HTTP_STATUS_BAD_REQUEST: '(Bad Request)',
        lcb.LCB_HTTP_STATUS_UNAUTHORIZED:'(Unauthorized)',
        lcb.LCB_HTTP_STATUS_PAYMENT_REQUIRED: '(Payment Required)',
        lcb.LCB_HTTP_STATUS_FORBIDDEN: '(Forbidden)',
        lcb.LCB_HTTP_STATUS_NOT_FOUND: '(Not Found)',
        lcb.LCB_HTTP_STATUS_METHOD_NOT_ALLOWED: '(Method Not Allowed)',
        lcb.LCB_HTTP_STATUS_NOT_ACCEPTABLE: '(Not Acceptable)',
        lcb.LCB_HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED:
            '(Proxy Authentication Required)',
        lcb.LCB_HTTP_STATUS_REQUEST_TIMEOUT: '(Request Timeout)',
        lcb.LCB_HTTP_STATUS_CONFLICT: '(Conflict)',
        lcb.LCB_HTTP_STATUS_GONE: '(Gone)',
        lcb.LCB_HTTP_STATUS_LENGTH_REQUIRED: '(Length Required)',
        lcb.LCB_HTTP_STATUS_PRECONDITION_FAILED: '(Precondition Failed)',
        lcb.LCB_HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE:
            '(Request Entity Too Large)',
        lcb.LCB_HTTP_STATUS_REQUEST_URI_TOO_LONG: '(Request Uri Too Long)',
        lcb.LCB_HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE:
            '(Unsupported Media Type)',
        lcb.LCB_HTTP_STATUS_REQUESTED_RANGE_NOT_SATISFIABLE:
            '(Requested Range Not Satisfiable)',
        lcb.LCB_HTTP_STATUS_EXPECTATION_FAILED: '(Expectation Failed)',
        lcb.LCB_HTTP_STATUS_UNPROCESSABLE_ENTITY: '(Unprocessable Entity)',
        lcb.LCB_HTTP_STATUS_LOCKED: '(Locked)',
        lcb.LCB_HTTP_STATUS_FAILED_DEPENDENCY: '(Failed Dependency)',
        lcb.LCB_HTTP_STATUS_INTERNAL_SERVER_ERROR: '(Internal Server Error)',
        lcb.LCB_HTTP_STATUS_NOT_IMPLEMENTED: '(Not Implemented)',
        lcb.LCB_HTTP_STATUS_BAD_GATEWAY: '(Bad Gateway)',
        lcb.LCB_HTTP_STATUS_SERVICE_UNAVAILABLE: '(Service Unavailable)',
        lcb.LCB_HTTP_STATUS_GATEWAY_TIMEOUT: '(Gateway Timeout)',
        lcb.LCB_HTTP_STATUS_HTTP_VERSION_NOT_SUPPORTED:
            '(Http Version Not Supported)',
        lcb.LCB_HTTP_STATUS_INSUFFICIENT_STORAGE: '(Insufficient Storage)'
    }

    def __init__(self, msg, error=0, key=None, status=0, cas=None,
                 operation=None):
        self.msg = msg
        self.error = error
        self.key = key
        self.status = status
        self.cas = cas
        self.operation = operation

    def __str__(self):
        info = []
        if self.error:
            info.append('error=0x{0:02x}'.format(self.error))
        if self.key:
            info.append('key={0}'.format(self.key))
        if self.status:
            info.append('status={0} {1}'.format(
                self.status, self.http_status_msg[self.status]))
        if self.cas:
            info.append('cas={0}'.format(self.cas))
        if self.operation:
            info.append('operation={0}'.format(self.operation))
        return '{0} ({1})'.format(self.msg, ', '.join(info))


# The exceptions need CouchbaseError(), hence import it afterwards
from couchbase import exceptions

include "connection.pyx"
