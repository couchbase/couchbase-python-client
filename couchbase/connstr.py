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

try:
    from urlparse import urlparse, parse_qs
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, parse_qs, urlencode

class ConnectionString(object):
    """
    This module is somewhat internal to help us parse the connection
    string for the tests
    """

    @classmethod
    def from_hb(self, host, bucket):
        ss = 'couchbase://{0}/{1}'.format(host, bucket)
        return self(ss)


    def __init__(self, ss):
        self._base = ss
        up = urlparse(ss)
        pthstr = up.path

        if '?' in pthstr:
            pthstr, qstr = up.path.split('?')
        else:
            qstr = ""

        if pthstr.startswith('/'):
            pthstr = pthstr[1:]

        self.bucket = pthstr
        self.options = parse_qs(qstr)
        self.scheme = up.scheme
        self.hosts = up.netloc.split(',')

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

    def encode(self):
        opt_dict = {}
        for k, v in self.options.items():
            opt_dict[k] = v[0]

        ss = '{scheme}://{hosts}/{bucket}?{options}'.format(
            scheme=self.scheme, hosts=','.join(self.hosts), bucket=self.bucket,
            options=urlencode(opt_dict))
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
    cs = ConnectionString(sample)
    print("Hosts", cs.hosts)
    print("Implicit Port", cs.implicit_port)
    print("Bucket", cs.bucket)
    print("Options", cs.options)

    cs.bucket = "Hi"
    print("Encoded again", cs)

    kwargs = convert_1x_args('beer-sample', host=[('192.168.37.101',8091)])
    print(kwargs)
