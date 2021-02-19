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

from subprocess import Popen, PIPE
from couchbase_core._pyport import urlopen, ulp, basestring
import socket
import json
import os.path
import select

class BucketSpec(object):
    def __init__(self, name='default', bucket_type='couchbase', password=''):
        self.name = name
        self.bucket_type = bucket_type
        self.password = password

    def __str__(self):
        return ':'.join([self.name, self.password, self.bucket_type])

class MockControlClient(object):
    def __init__(self, mockport=18091, host='127.0.0.1'):
        self.urlbase = "http://{0}:{1}/mock/".format(host, mockport)

    def _get_common(self, command, params):
        qparams = {}
        for k, v in params.items():
            qparams[k] = json.dumps(v)
        qparams = ulp.urlencode(qparams)
        url = self.urlbase + "{0}?{1}".format(command, qparams)
        data = urlopen(url).read()
        if not isinstance(data, basestring):
            data = str(data, "utf-8")
        ret = json.loads(data)
        return ret

    def _params_common(self, key,
                       bucket=None,
                       on_master=False,
                       replica_count=None,
                       replicas=None,
                       cas=None,
                       value=None):
        r = {
            'Key' : key,
            'OnMaster' : on_master
        }
        if bucket:
            r['Bucket'] = bucket
        if replica_count is not None:
            r['OnReplicas'] = replica_count
        else:
            r['OnReplicas'] = replicas
        if cas is not None:
            r['CAS'] = cas

        if value is not None:
            r['Value'] = value

        return r

    def _do_request(self, cmd, *args, **kwargs):
        params = self._params_common(*args, **kwargs)
        return self._get_common(cmd, params)

    def keyinfo(self, *args, **kwargs):
        return self._do_request("keyinfo", *args, **kwargs)

    def persist(self, *args, **kwargs):
        return self._do_request("persist", *args, **kwargs)

    def endure(self, *args, **kwargs):
        return self._do_request("endure", *args, **kwargs)

    def cache(self, *args, **kwargs):
        return self._do_request("cache", *args, **kwargs)

    def uncache(self, *args, **kwargs):
        return self._do_request("uncache", *args, **kwargs)

    def unpersist(self, *args, **kwargs):
        return self._do_request("unpersist", *args, **kwargs)

    def purge(self, *args, **kwargs):
        return self._do_request("purge", *args, **kwargs)


class CouchbaseMock(object):
    def _setup_listener(self):
        sock = socket.socket()
        sock.bind( ('', 0) )
        sock.listen(5)

        addr, port = sock.getsockname()
        self.listen = sock
        self.port = port

    def _invoke(self):
        self._setup_listener()
        args = [
            "java", "-client", "-jar", self.runpath,
            "--port", "0", "--with-beer-sample",
            "--harakiri-monitor", "127.0.0.1:" + str(self.port),
            "--nodes", str(self.nodes)
        ]

        if self.vbuckets is not None:
            args += ["--vbuckets", str(self.vbuckets)]

        if self.replicas is not None:
            args += ["--replicas", str(self.replicas)]

        bspec = ",".join([str(x) for x in self.buckets])
        args += ["--buckets", bspec]

        self.po = Popen(args)

        # Sometimes we get an invalid JAR file. Unfortunately there is no
        # way to determine or "wait for completion". The next best thing
        # is to set a maximum of 15 seconds for the process to start (and
        # connect to the listening socket);

        rlist, w_, x_ = select.select([self.listen], [], [], 15)
        if not rlist:
            raise Exception('Mock server was not ready in time')

        self.harakiri_sock, addr = self.listen.accept()
        self.ctlfp = self.harakiri_sock.makefile()

        sbuf = ""
        while True:
            c = self.ctlfp.read(1)
            if c == '\0':
                break
            sbuf += c
        self.rest_port = int(sbuf)

    def __init__(self, buckets, runpath,
                 url=None,
                 replicas=None,
                 vbuckets=None,
                 nodes=4):
        """
        Creates a new instance of the mock server. You must actually call
        'start()' for it to be invoked.
        :param list buckets: A list of BucketSpec items
        :param string runpath: a string pointing to the location of the mock
        :param string url: The URL to use to download the mock. This is only
          used if runpath does not exist
        :param int replicas: How many replicas should each bucket have
        :param int vbuckets: How many vbuckets should each bucket have
        :param int nodes: How many total nodes in the cluster

        Note that you must have ``java`` in your `PATH`
        """


        self.runpath = runpath
        self.buckets = buckets
        self.nodes = nodes
        self.vbuckets = vbuckets
        self.replicas = replicas

        if not os.path.exists(runpath):
            if not url:
                raise Exception(runpath + " Does not exist and no URL specified")
            fp = open(runpath, "wb")
            ulp = urlopen(url)
            jarblob = ulp.read()
            fp.write(jarblob)
            fp.close()

    def start(self):
        self._invoke()

    def stop(self):
        '''
            PYCBC - 1097
            Allow for clean shutdown of mock in order to create another mock
        '''
        try:
            self.listen.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self.listen.close()

        try:
            self.harakiri_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self.harakiri_sock.close()

        try:
            self.po.terminate()
            self.po.kill()
            self.po.communicate()
        except OSError:
            pass


