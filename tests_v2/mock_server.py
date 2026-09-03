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
import socket
import sys
import time
from enum import IntEnum
from subprocess import (DEVNULL,
                        STDOUT,
                        Popen,
                        call)
from typing import Any
from urllib.request import urlretrieve
from uuid import uuid4


def _worker_suffix() -> str:
    """`-gw0` under xdist, empty otherwise."""
    worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
    return f'-{worker_id}' if worker_id else ''


class MockServerException(Exception):
    pass


class MockServerType(IntEnum):
    """Which mock a run is talking to.  GoCAVES is currently the only one.

    Kept as an enum with a single member rather than collapsed away, because every
    call site already reads `mock_server_type == MockServerType.GoCAVES` and
    `mock_server_type is None` to mean "real server".  A second mock adds a member
    here and changes nothing else.
    """
    GoCAVES = 1


class MockServer:

    TEST_DIR = pathlib.Path(__file__).parent

    def __init__(self, log_dir: str | None = None) -> None:
        """*log_dir* both enables the mock's own log and says where to put it.

        The mock's stdout is the only account of why it failed to start: `start()` can
        report no more than that the greeting never arrived.  Nothing reads the file
        otherwise, so it is off unless a run asks for it.

        One file per xdist worker.  Sharing one path across workers loses all but one of
        them, since each opens it truncating.
        """
        self._rest_port: int | None = None
        self._connstr: str | None = None
        self._log_filename: str | None = None

        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            self._log_filename = os.path.join(log_dir, f'gocaves{_worker_suffix()}.log')

    # Both raise rather than return None.  They are only meaningful after start()
    # (and create_cluster(), for GoCAVES) has populated them, and every caller uses
    # the value to build a connection string.  A None reaching that far becomes the
    # literal text "None" inside a URL, which fails much later and somewhere else.
    @property
    def rest_port(self) -> int:
        if self._rest_port is None:
            raise MockServerException('Mock server has no REST port; start() did not run or failed.')
        return self._rest_port

    @property
    def connstr(self) -> str:
        if self._connstr is None:
            raise MockServerException('Mock server has no connection string; create_cluster() did not run.')
        return self._connstr

    @property
    def mock_type(self) -> MockServerType:
        raise NotImplementedError("Mock doesn't implement mock_type.")

    def start(self) -> None:
        raise NotImplementedError("Mock doesn't implement start().")

    def shutdown(self) -> None:
        raise NotImplementedError("Mock doesn't implement shutdown().")

    @staticmethod
    def create_caves_mock_server(
        caves_path: str | None = None,
        caves_url: str | None = None,
        caves_version: str | None = None,
        log_dir: str | None = None,
    ) -> CavesMockServer:
        return CavesMockServer(caves_path=caves_path,
                               caves_url=caves_url,
                               caves_version=caves_version,
                               log_dir=log_dir)


class CavesMockServer(MockServer):
    def __init__(self,
                 caves_path: str | None = None,
                 caves_url: str | None = None,
                 caves_version: str | None = None,
                 log_dir: str | None = None,
                 ) -> None:

        super().__init__(log_dir=log_dir)

        if caves_url is None:
            caves_url = 'https://github.com/couchbaselabs/gocaves/releases/download'

        self._caves_version = caves_version
        if self._caves_version is None:
            self._caves_version = 'v0.0.1-74'

        self._build_caves_url(caves_url)
        self._validate_caves_path(caves_path)

    @staticmethod
    def ensure_binary(caves_path: str | None = None,
                      caves_url: str | None = None,
                      caves_version: str | None = None) -> str:
        """Download the binary if it is absent, and return where it landed.

        Constructing a mock is what fetches the binary; nothing runs until `start()`.
        A run calls this once, before xdist forks, so that N workers share one 16MB
        fetch rather than each making their own.
        """
        mock = CavesMockServer(caves_path=caves_path,
                               caves_url=caves_url,
                               caves_version=caves_version)
        return mock._caves_path

    @property
    def mock_type(self) -> MockServerType:
        return MockServerType.GoCAVES

    def _build_caves_url(self, url: str) -> None:

        if sys.platform.startswith('linux'):
            if platform.machine() == 'aarch64':
                self._caves_url = f"{url}/{self._caves_version}/gocaves-linux-arm64"
            else:
                self._caves_url = f"{url}/{self._caves_version}/gocaves-linux-amd64"
        elif sys.platform.startswith('darwin'):
            if platform.machine() == 'arm64':
                self._caves_url = f"{url}/{self._caves_version}/gocaves-macos-arm64"
            else:
                self._caves_url = f"{url}/{self._caves_version}/gocaves-macos"
        elif sys.platform.startswith('win32'):
            self._caves_url = f"{url}/{self._caves_version}/gocaves-windows.exe"
        else:
            raise MockServerException("Unrecognized platform for running GoCAVES mock server.")

    def _validate_caves_path(self, caves_path: str | None = None) -> None:
        if not (caves_path and not caves_path.isspace()):
            if sys.platform.startswith('linux'):
                caves_path = 'gocaves-linux-arm64' if platform.machine() == 'aarch64' else 'gocaves-linux-amd64'
            elif sys.platform.startswith('darwin'):
                caves_path = 'gocaves-macos-arm64' if platform.machine() == 'arm64' else 'gocaves-macos'
            elif sys.platform.startswith('win32'):
                caves_path = 'gocaves-windows.exe'

        self._caves_path = str(self.TEST_DIR.parent.joinpath(caves_path))

        if not os.path.exists(self._caves_path):
            self._download_caves()

    def _download_caves(self) -> None:
        """Fetch the binary under a private name, then rename it into place.

        `os.path.exists` on the final path is the whole guard on this download, and
        writing straight to that path makes it true from the first byte of a 16MB
        transfer.  Every xdist worker is its own pytest session and checks for itself,
        so one arriving mid-transfer finds the file, skips the fetch and execs a
        partial binary.  `os.replace` is atomic, so the name appears only once the
        file is complete and executable.

        Local runs never see this: the binary is already there.  It is a cold machine
        that races, which is CI.
        """
        tmp_path = f'{self._caves_path}.{os.getpid()}.tmp'
        try:
            urlretrieve(self._caves_url, tmp_path)
            if not sys.platform.startswith('win32'):
                call(['chmod', 'a+x', tmp_path])
            try:
                os.replace(tmp_path, self._caves_path)
            except OSError:
                # Another worker got there first, and on Windows its copy may already be
                # running, which refuses to be replaced.  Both copies are the same release
                # asset, so losing the race is as good as winning it.
                if not os.path.exists(self._caves_path):
                    raise
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _setup_listener(self) -> None:
        sock = socket.socket()
        sock.bind(('', 0))
        sock.listen(10)

        _, port = sock.getsockname()
        self._listen = sock
        self._port = port

    def _invoke(self) -> None:
        self._setup_listener()
        args = [self._caves_path, f"--control-port={self._port}"]
        self._output_log = open(self._log_filename, 'w') if self._log_filename else None
        self._po = Popen(args, stdout=self._output_log or DEVNULL, stderr=STDOUT)
        self._caves_sock, self._caves_addr = self._listen.accept()
        self._rest_port = self._caves_addr[1]

    def _attempt_shutdown(self) -> None:
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
            if self._output_log is not None:
                self._output_log.close()
            self._po.terminate()
            self._po.kill()
            self._po.communicate()
        except OSError:
            pass

    def start(self) -> None:
        self._invoke()
        hello = self._read_command()
        if hello['type'] != 'hello':
            raise MockServerException("There was a problem, CAVES didn't greet us.")

    def shutdown(self) -> None:
        self._attempt_shutdown()

    def create_cluster(self) -> None:
        self._cluster_id = str(uuid4())
        res = self._round_trip_command({
            'type': 'createcluster',
            'id': self._cluster_id
        })
        if res is not None:
            self._connstr = res['connstr']
            self._mgmt_addrs = res['mgmt_addrs']

    def _read_command(self) -> dict[str, Any]:
        result = self._recv()
        return json.loads(result)

    def _write_command(self, cmd: dict[str, Any]) -> None:
        cmd_str = json.dumps(cmd)
        cmd_array = bytearray(cmd_str, 'utf-8')
        # append null byte
        cmd_array += b'\x00'
        self._caves_sock.send(bytes(cmd_array))

    def _round_trip_command(self, cmd: dict[str, Any]) -> dict[str, Any]:
        self._write_command(cmd)
        resp = self._read_command()
        return resp

    def _recv(self, timeout: float = 2, end: bytes = b'\x00') -> str:
        self._caves_sock.setblocking(False)
        buf: list[str] = []
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
