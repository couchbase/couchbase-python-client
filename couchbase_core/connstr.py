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
from __future__ import print_function
import copy

try:
    from urlparse import urlparse, parse_qs
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, parse_qs, urlencode


class ConnectionString(object):
    """
    This class parses the connection string and may be passed
    to the :class:`~.Bucket` constructor instead of a raw
    string.

    .. note::

        Raw strings passed to the :class:`~.Bucket` constructor
        are *not* first converted into a `ConnectionString`
        object. This is an internal implementation detail, but
        may be helpful to know in case of bugs encountered in
        this class.

    The :meth:`encode` method can be used to check the encoded
    form of the connection string.
    """

    def __init__(self, bucket='default',
                 hosts=None, options=None, scheme='couchbase'):
        """
        Create a new ConnectionString object.

        This is an alternative form to manually inputting
        a connection string.

        :param string bucket: The bucket to connect to
        :param list hosts: A list of hosts to which the initial
            connection should be attempted
        :param dict options: A dictionary of options. These options
            are passed verbatim to the C library.
        :param string scheme: The scheme to be used when connecting.
            This scheme is used to interpret the initial default
            port to use for each node.
        """

        #: The bucket to connect to. See :meth:`~.__init__`
        self.bucket = bucket

        #: Options for the connection. See :meth:`~.__init__`
        self.options = copy.copy(options) if options else {}

        #: List of hosts. See :meth:`~.__init__`
        self.hosts = copy.copy(hosts) if hosts else []

        #: The scheme to be used. See :meth:`~.__init__`
        self.scheme = scheme

    @classmethod
    def parse(cls, ss):
        """
        Parses an existing connection string

        This method will return a :class:`~.ConnectionString` object
        which will allow further inspection on the input parameters.

        :param string ss: The existing connection string
        :return: A new :class:`~.ConnectionString` object
        """

        up = urlparse(ss)
        path = up.path
        query = up.query

        if '?' in path:
            path, _ = up.path.split('?')

        if path.startswith('/'):
            path = path[1:]

        bucket = path
        options = parse_qs(query)
        scheme = up.scheme
        hosts = up.netloc.split(',')
        return cls(bucket=bucket, options=options, hosts=hosts, scheme=scheme)

    @property
    def implicit_port(self):
        if self.scheme == 'http':
            return 8091
        elif self.scheme == 'couchbase':
            return 11210
        elif self.scheme == 'couchbases':
            return 11207
        else:
            return -1

    def get_option(self, optname, default=None):
        try:
            return self.options[optname][0]
        except KeyError:
            return default

    def set_option(self, optname, value):
        self.options[optname] = [value]

    def clear_option(self, optname):
        self.options.pop(optname, None)

    def encode(self):
        """
        Encodes the current state of the object into a string.

        :return: The encoded string
        """
        opt_dict = {}
        for k, v in self.options.items():
            opt_dict[k] = v[0]

        ss = '{0}://{1}'.format(self.scheme, ','.join(self.hosts))
        if self.bucket:
            ss += '/' + self.bucket

        # URL encode options then decoded forward slash /
        ss += '?' + urlencode(opt_dict).replace('%2F', '/')

        return ss

    def __str__(self):
        return self.encode()


def _fmthost(host, port):
    if port is not None:
        return '{0}:{1}'.format(host, port)
    else:
        return host


def _build_connstr(host, port, bucket):
    """
    Converts a 1.x host:port specification to a connection string
    """
    hostlist = []
    if isinstance(host, (tuple, list)):
        for curhost in host:
            if isinstance(curhost, (list, tuple)):
                hostlist.append(_fmthost(*curhost))
            else:
                hostlist.append(curhost)
    else:
        hostlist.append(_fmthost(host, port))

    return 'http://{0}/{1}'.format(','.join(hostlist), bucket)


def convert_1x_args(bucket, **kwargs):
    """
    Converts arguments for 1.x constructors to their 2.x forms
    """
    host = kwargs.pop('host', 'localhost')
    port = kwargs.pop('port', None)
    if not 'connstr' in kwargs and 'connection_string' not in kwargs:
        kwargs['connection_string'] = _build_connstr(host, port, bucket)
    return kwargs


if __name__ == "__main__":
    sample = "couchbase://host1:111,host2:222,host3:333/default?op_timeout=4.2"
    cs = ConnectionString.parse(sample)
    print("Hosts", cs.hosts)
    print("Implicit Port", cs.implicit_port)
    print("Bucket", cs.bucket)
    print("Options", cs.options)

    cs.bucket = "Hi"
    print("Encoded again", cs)

    kwargs = convert_1x_args('beer-sample', host=[('192.168.37.101',8091)])
    print(kwargs)
