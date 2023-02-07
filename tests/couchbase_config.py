#  Copyright 2016-2023. Couchbase, Inc.
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

import os
import pathlib
import time
from configparser import ConfigParser
from typing import Optional

import pytest

from tests.environments import CouchbaseTestEnvironmentException

from .mock_server import (LegacyMockBucketSpec,
                          MockServer,
                          MockServerType)

BASEDIR = pathlib.Path(__file__).parent.parent
CONFIG_FILE = os.path.join(pathlib.Path(__file__).parent, "test_config.ini")


class CouchbaseConfig:
    def __init__(self):
        self.host = "localhost"
        self.port = 8091
        self.admin_username = "Administrator"
        self.admin_password = "password"
        self.bucket_name = "default"
        self.bucket_password = ""
        self.real_server_enabled = False
        self.mock_server_enabled = False
        self.mock_server = None
        self.protostellar_enabled = False

    @property
    def is_mock_server(self):
        return self.mock_server_enabled

    @property
    def is_real_server(self):
        return self.real_server_enabled

    @property
    def is_protostellar(self):
        return self.protostellar_enabled

    def get_connection_string(self) -> str:
        if self.mock_server_enabled:
            if self.mock_server.mock_type == MockServerType.Legacy:
                # What current client uses for mock:
                # http://127.0.0.1:49696?ipv6=disabled
                return f"http://{self.host}:{self.port}"

            return self.mock_server.connstr
        elif self.is_protostellar:
            return f"protostellar://{self.host}:{self.port}"
        else:
            return f"couchbase://{self.host}"

    def get_username_and_pw(self):
        return self.admin_username, self.admin_password

    def shutdown(self):
        if self.mock_server_enabled and self.mock_server.mock_type == MockServerType.GoCAVES:
            self.mock_server.shutdown()

    @classmethod
    def load_config(cls):  # noqa: C901
        couchbase_config = cls()
        try:
            test_config = ConfigParser()
            test_config.read(CONFIG_FILE)

            if test_config.getboolean('realserver', 'enabled', fallback=False):
                couchbase_config.real_server_enabled = True
                couchbase_config.protostellar_enabled = test_config.getboolean(
                    'realserver', 'is_protostellar', fallback=False)
                couchbase_config.host = test_config.get('realserver', 'host')
                couchbase_config.port = test_config.getint('realserver', 'port')
                couchbase_config.admin_username = test_config.get(
                    'realserver', 'admin_username')
                couchbase_config.admin_password = test_config.get(
                    'realserver', 'admin_password')
                couchbase_config.bucket_name = test_config.get('realserver', 'bucket_name')
                couchbase_config.bucket_password = test_config.get(
                    'realserver', 'bucket_password')

            mock_path = ''
            mock_url = ''
            mock_version = ''
            # @TODO(jc):  allow override of log dir and filename
            # log_dir = ''
            # log_filename = ''
            if test_config.getboolean('gocaves', 'enabled'):
                if couchbase_config.real_server_enabled:
                    raise CouchbaseTestEnvironmentException(
                        "Both real and mock servers cannot be enabled at the same time.")

                couchbase_config.mock_server_enabled = True

                if test_config.has_option('gocaves', 'path'):
                    mock_path = str(
                        BASEDIR.joinpath(test_config.get('gocaves', 'path')))
                if test_config.has_option('gocaves', 'url'):
                    mock_url = test_config.get('gocaves', 'url')
                if test_config.has_option('gocaves', 'version'):
                    mock_version = test_config.get('gocaves', 'version')

                couchbase_config.mock_server = CouchbaseConfig.create_mock_server(MockServerType.GoCAVES,
                                                                                  mock_path,
                                                                                  mock_url,
                                                                                  mock_version)
                couchbase_config.bucket_name = "default"
                # cluster_info.port = cluster_info.mock_server.rest_port
                # cluster_info.host = "127.0.0.1"
                couchbase_config.admin_username = "Administrator"
                couchbase_config.admin_password = "password"

            if test_config.has_section('legacymockserver') and test_config.getboolean('legacymockserver', 'enabled'):
                if couchbase_config.real_server_enabled:
                    raise CouchbaseTestEnvironmentException(
                        "Both real and mock servers cannot be enabled at the same time.")

                if couchbase_config.mock_server_enabled:
                    raise CouchbaseTestEnvironmentException(
                        "Both java mock and gocaves mock cannot be enabled at the same time.")

                couchbase_config.mock_server_enabled = True
                mock_path = str(
                    BASEDIR.joinpath(test_config.get("mockserver", "path")))
                if test_config.has_option("mockserver", "url"):
                    mock_url = test_config.get("mockserver", "url")

                couchbase_config.mock_server = CouchbaseConfig.create_mock_server(MockServerType.Legacy,
                                                                                  mock_path,
                                                                                  mock_url)
                couchbase_config.bucket_name = "default"
                couchbase_config.port = couchbase_config.mock_server.rest_port
                couchbase_config.host = "127.0.0.1"
                couchbase_config.admin_username = "Administrator"
                couchbase_config.admin_password = "password"

        except CouchbaseTestEnvironmentException:
            raise
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(
                f"Problem trying read/load test configuration:\n{ex}")

        return couchbase_config

    @staticmethod
    def create_mock_server(mock_type,  # type: MockServerType
                           mock_path,  # type: str
                           mock_download_url,  # type: Optional[str]
                           mock_version,  # type: Optional[str]
                           log_dir=None,  # type: Optional[str]
                           log_filename=None,  # type: Optional[str]
                           ) -> MockServer:

        if mock_type == MockServerType.Legacy:
            bspec_dfl = LegacyMockBucketSpec('default', 'couchbase')
            mock = MockServer.create_legacy_mock_server([bspec_dfl],
                                                        mock_path,
                                                        mock_download_url,
                                                        replicas=2,
                                                        nodes=4)
        else:
            mock = MockServer.create_caves_mock_server(mock_path,
                                                       mock_download_url,
                                                       mock_version,
                                                       log_dir,
                                                       log_filename)

        try:
            mock.start()
            if mock_type == MockServerType.GoCAVES:
                mock.create_cluster()
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(
                f"Problem trying to start mock server:\n{ex}")

        return mock

    @staticmethod
    def restart_mock(mock) -> None:
        try:
            print("\nR.I.P. mock...")
            mock.stop()
            time.sleep(3)
            mock.start()
            return mock
        except Exception:
            import traceback
            traceback.print_exc()
            raise CouchbaseTestEnvironmentException('Error trying to restart mock')


@pytest.fixture(scope="session")
def couchbase_test_config():
    config = CouchbaseConfig.load_config()
    yield config
    config.shutdown()
