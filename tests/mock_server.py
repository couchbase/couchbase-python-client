#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import json
import os
import pathlib
import platform
import select
import socket
import sys
import time
from enum import IntEnum
from subprocess import (STDOUT,
                        Popen,
                        call)
from typing import List, Optional
from urllib.request import urlretrieve
from uuid import uuid4


class MockServerException(Exception):
    pass


class MockServerType(IntEnum):
    GoCAVES = 1,
    Legacy = 2


class MockServer:

    TEST_DIR = pathlib.Path(__file__).parent

    def __init__(self, log_dir=None, log_filename=None):
        self._rest_port = None
        self._connstr = None

        if log_dir is None:
            self._log_dir = os.path.join(self.TEST_DIR, "test_logs")
        else:
            self._log_dir = log_dir

        if not os.path.exists(self._log_dir):
            os.mkdir(self._log_dir)

        if log_filename is None:
            self._log_filename = os.path.join(self._log_dir, 'test_logs.txt')
        else:
            self._log_filename = log_filename

    @property
    def rest_port(self):
        return self._rest_port

    @property
    def connstr(self):
        return self._connstr

    def start(self):
        raise NotImplementedError("Mock doesn't implement start().")

    def shutdown(self):
        raise NotImplementedError("Mock doesn't implement shutdown().")

    @staticmethod
    def create_legacy_mock_server(buckets,  # type: List[LegacyMockBucketSpec]
                                  jar_path,  # type: str
                                  url=None,  # type: str
                                  replicas=None,  # type: int
                                  vbuckets=None,  # type: int
                                  nodes=4  # type: int
                                  ) -> LegacyMockServer:
        return LegacyMockServer(buckets=buckets,
                                jar_path=jar_path,
                                url=url,
                                replicas=replicas,
                                vbuckets=vbuckets,
                                nodes=nodes
                                )

    @staticmethod
    def create_caves_mock_server(caves_path=None,  # type: Optional[str]
                                 caves_url=None,  # type: Optional[str]
                                 caves_version=None,  # type: Optional[str]
                                 log_dir=None,  # type: Optional[str]
                                 log_filename=None,  # type: Optional[str]
                                 ) -> CavesMockServer:
        return CavesMockServer(caves_path=caves_path,
                               caves_url=caves_url,
                               caves_version=caves_version,
                               log_dir=log_dir,
                               log_filename=log_filename)


class LegacyMockBucketSpec():
    def __init__(self,
                 name='default',  # type: str
                 bucket_type='couchbase',  # type: str
                 password='',  # type: str
                 ):
        self._name = name
        self._bucket_type = bucket_type
        self._password = password

    def __str__(self):
        return ':'.join([self._name, self._password, self._bucket_type])


class LegacyMockServer(MockServer):

    def __init__(self,
                 buckets,  # type: List[LegacyMockBucketSpec]
                 jar_path,  # type: str
                 url=None,  # type: str
                 replicas=None,  # type: int
                 vbuckets=None,  # type: int
                 nodes=4  # type: int
                 ):
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

        super().__init__()
        self._validate_jar(jar_path, url)
        self._jar_path = jar_path
        self._url = url
        self._buckets = buckets
        self._nodes = nodes
        self._vbuckets = vbuckets
        self._replicas = replicas

    # @property
    # def rest_port(self):
    #     return self._rest_port
    @property
    def mock_type(self) -> MockServerType:
        return MockServerType.Legacy

    def _validate_jar(self, jar_path, url):
        if not os.path.exists(jar_path):
            if not url:
                raise MockServerException(
                    f"{jar_path} does not exist and no URL specified.")
            # fp = open(runpath, "wb")
            # ulp = urlopen(url)
            # jarblob = ulp.read()
            # fp.write(jarblob)
            # fp.close()
            urlretrieve(url, jar_path)

    def _setup_listener(self):
        sock = socket.socket()
        sock.bind(('', 0))
        sock.listen(5)

        _, port = sock.getsockname()
        self._listen = sock
        self._port = port

    def _invoke(self):
        self._setup_listener()
        args = [
            "java", "-client", "-jar", self._runpath,
            "--port", "0", "--with-beer-sample",
            "--harakiri-monitor", "127.0.0.1:" + str(self._port),
            "--nodes", str(self._nodes)
        ]

        if self._vbuckets is not None:
            args += ["--vbuckets", str(self._vbuckets)]

        if self._replicas is not None:
            args += ["--replicas", str(self._replicas)]

        bspec = ",".join([str(x) for x in self._buckets])
        args += ["--buckets", bspec]

        self._po = Popen(args)

        # Sometimes we get an invalid JAR file. Unfortunately there is no
        # way to determine or "wait for completion". The next best thing
        # is to set a maximum of 15 seconds for the process to start (and
        # connect to the listening socket);

        rlist, _, _ = select.select([self._listen], [], [], 15)
        if not rlist:
            raise MockServerException(
                'Mock server was not ready in time')

        self._harakiri_sock, _ = self._listen.accept()
        self._ctlfp = self._harakiri_sock.makefile()

        sbuf = ""
        while True:
            c = self._ctlfp.read(1)
            if c == '\0':
                break
            sbuf += c
        self._rest_port = int(sbuf)

    def _attempt_shutdown(self):
        try:
            self._listen.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self._listen.close()

        try:
            self._harakiri_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self._harakiri_sock.close()

        try:
            self._po.terminate()
            self._po.kill()
            self._po.communicate()
        except OSError:
            pass

    def start(self):
        self._invoke()

    def shutdown(self):
        self._attempt_shutdown()


class CavesMockServer(MockServer):
    def __init__(self,
                 caves_path=None,  # type: Optional[str]
                 caves_url=None,  # type: Optional[str]
                 caves_version=None,  # type: Optional[str]
                 log_dir=None,  # type: Optional[str]
                 log_filename=None,  # type: Optional[str]
                 ):

        super().__init__(log_dir=log_dir, log_filename=log_filename)

        if caves_url is None:
            caves_url = 'https://github.com/couchbaselabs/gocaves/releases/download'

        self._caves_version = caves_version
        if self._caves_version is None:
            self._caves_version = 'v0.0.1-74'

        self._build_caves_url(caves_url)
        self._validate_caves_path(caves_path)

    # @property
    # def rest_port(self):
    #     return self._rest_port

    # @property
    # def connstr(self):
    #     return self._connstr

    @property
    def mock_type(self) -> MockServerType:
        return MockServerType.GoCAVES

    def _build_caves_url(self, url):

        if sys.platform.startswith('linux'):
            if platform.machine() == 'aarch64':
                self._caves_url = f"{url}/{self._caves_version}/gocaves-linux-arm64"
            else:
                self._caves_url = f"{url}/{self._caves_version}/gocaves-linux-amd64"
        elif sys.platform.startswith('darwin'):
            self._caves_url = f"{url}/{self._caves_version}/gocaves-macos"
        elif sys.platform.startswith('win32'):
            self._caves_url = f"{url}/{self._caves_version}/gocaves-windows.exe"
        else:
            raise MockServerException("Unrecognized platform for running GoCAVES mock server.")

    def _validate_caves_path(self, caves_path=None):
        if not (caves_path and not caves_path.isspace()):
            if sys.platform.startswith('linux'):
                caves_path = 'gocaves-linux-arm64' if platform.machine() == 'aarch64' else 'gocaves-linux-amd64'
            elif sys.platform.startswith('darwin'):
                caves_path = 'gocaves-macos'
            elif sys.platform.startswith('win32'):
                caves_path = 'gocaves-windows.exe'

        self._caves_path = str(self.TEST_DIR.parent.joinpath(caves_path))

        if not os.path.exists(self._caves_path):
            urlretrieve(self._caves_url, self._caves_path)
            if not sys.platform.startswith('win32'):
                # make executable
                call(['chmod', 'a+x', self._caves_path])

    def _setup_listener(self):
        sock = socket.socket()
        sock.bind(('', 0))
        sock.listen(10)

        _, port = sock.getsockname()
        self._listen = sock
        self._port = port

    def _invoke(self):
        self._setup_listener()
        args = [self._caves_path, f"--control-port={self._port}"]
        self._output_log = open(self._log_filename, 'w')
        self._po = Popen(args, stdout=self._output_log, stderr=STDOUT)
        self._caves_sock, self._caves_addr = self._listen.accept()
        self._rest_port = self._caves_addr[1]

    def _attempt_shutdown(self):
        try:
            self._listen.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self._listen.close()

        try:
            self._caves_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self._caves_sock.close()

        try:
            self._output_log.close()
            self._po.terminate()
            self._po.kill()
            self._po.communicate()
        except OSError:
            pass

    def start(self):
        self._invoke()
        hello = self._read_command()
        if hello['type'] != 'hello':
            raise MockServerException("There was a problem, CAVES didn't greet us.")

    def shutdown(self):
        self._attempt_shutdown()

    def create_cluster(self):
        self._cluster_id = str(uuid4())
        res = self._round_trip_command({
            'type': 'createcluster',
            'id': self._cluster_id
        })
        if res is not None:
            self._connstr = res['connstr']
            self._mgmt_addrs = res['mgmt_addrs']

    def _read_command(self):
        result = self._recv()
        return json.loads(result)

    def _write_command(self, cmd):
        cmd_str = json.dumps(cmd)
        cmd_array = bytearray(cmd_str, 'utf-8')
        # append null byte
        cmd_array += b'\x00'
        self._caves_sock.send(bytes(cmd_array))

    def _round_trip_command(self, cmd):
        self._write_command(cmd)
        resp = self._read_command()
        return resp

    def _recv(self, timeout=2, end=b'\x00'):
        self._caves_sock.setblocking(0)
        buf = []
        data = ''
        chunk = 4096
        start = time.time()
        while True:
            # if received data and passed timeout
            # return the received data
            if buf and time.time() - start > timeout:
                break
            # if no data received, allow a bit more time
            elif time.time() - start > timeout * 1.1:
                break

            try:
                data = self._caves_sock.recv(chunk)
                if data:
                    if end and end in data:
                        buf.append(str(data[:len(data)-1], encoding='utf-8'))
                        break

                    buf.append(str(data, encoding='utf-8'))
                    start = time.time()
                else:
                    time.sleep(0.1)
            except Exception:
                pass

        result = ''.join(buf)
        return result
