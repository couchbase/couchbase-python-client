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

from __future__ import annotations

import os
import pathlib
import time
from collections.abc import Generator
from configparser import ConfigParser
from typing import NamedTuple

import pytest

from tests.environments import CouchbaseTestEnvironmentException
from tests.test_features import TestProtocol

from .mock_server import CavesMockServer, MockServer

BASEDIR = pathlib.Path(__file__).parent.parent
CONFIG_FILE = os.path.join(pathlib.Path(__file__).parent, "test_config.ini")

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------
# Config resolution order: env var > test_config.ini > hardcoded default.
# Set these in CI pipelines or developer shells to avoid editing test_config.ini.
_ENV_HOST = "PYCBC_HOST"
_ENV_PORT = "PYCBC_PORT"
_ENV_USERNAME = "PYCBC_USERNAME"
_ENV_PASSWORD = "PYCBC_PASSWORD"
_ENV_BUCKET = "PYCBC_BUCKET"
_ENV_SERVER_ENABLED = "PYCBC_SERVER_ENABLED"
_ENV_COUCHBASE2 = "PYCBC_COUCHBASE2"
_ENV_GOCAVES_ENABLED = "PYCBC_GOCAVES_ENABLED"
_ENV_GOCAVES_VERSION = "PYCBC_GOCAVES_VERSION"
_ENV_GOCAVES_PATH = "PYCBC_GOCAVES_PATH"
_ENV_GOCAVES_LOG_DIR = "PYCBC_GOCAVES_LOG_DIR"


def _env_bool(name: str) -> bool | None:
    """Return True/False if *name* is set in the environment, else None."""
    val = os.environ.get(name)
    if val is None:
        return None
    return val.strip().lower() in ("1", "true", "yes")


def _env_str(name: str) -> str | None:
    val = os.environ.get(name)
    return val.strip() if val else None


def _env_int(name: str) -> int | None:
    val = os.environ.get(name)
    if val is None:
        return None
    try:
        return int(val.strip())
    except ValueError:
        raise CouchbaseTestEnvironmentException(
            f"Environment variable {name!r} must be an integer; got {val!r}."
        )


def _section_enabled(env_name: str, ini: ConfigParser, section: str) -> tuple[bool, str]:
    """Resolve one section's `enabled` flag, and say which input decided it.

    The single place the env-var-shadows-ini precedence for `enabled` is written down.  Both
    loaders below call it, and so does describe_config_source(), which is the point: a header
    that reported the server independently of the loader could disagree with it, and a header
    that lies about which server a run reached is worse than no header at all.
    """
    from_env = _env_bool(env_name)
    if from_env is not None:
        return from_env, f'${env_name}'
    return ini.getboolean(section, "enabled", fallback=False), 'ini'


class ConfigSource(NamedTuple):
    """What a run resolved its server from, for reporting before anything connects."""

    path: str
    # 'real', 'mock', 'none' when neither section is enabled, or 'conflict' when both are.
    server: str
    # Which input decided `server`: 'ini', or '$PYCBC_...' when an env var shadowed the file.
    decided_by: str
    host: str | None
    port: int | None

    def summary(self) -> str:
        if self.server == 'real':
            where = f' host={self.host}:{self.port}'
        elif self.server == 'mock':
            where = ' (gocaves)'
        elif self.server == 'conflict':
            where = ' ([realserver] and [gocaves] are both enabled)'
        else:
            where = ' (neither [realserver] nor [gocaves] is enabled)'
        return f'{self.path}: server={self.server}{where} [{self.decided_by}]'


def describe_config_source() -> ConfigSource:
    """Answer which config file a run reads and which server it will reach, without side effects.

    Deliberately not CouchbaseConfig.load_config(): that starts GoCAVES as part of resolving
    the [gocaves] section, so calling it to build a header would start a second mock.  This
    reads the same file and the same env vars and constructs nothing.
    """
    ini = ConfigParser()
    ini.read(CONFIG_FILE)

    real_enabled, real_by = _section_enabled(_ENV_SERVER_ENABLED, ini, "realserver")
    mock_enabled, mock_by = _section_enabled(_ENV_GOCAVES_ENABLED, ini, "gocaves")

    # Reported rather than resolved to one of the two.  load_config() rejects this combination,
    # so answering 'real' here would print a header naming a server the run never reaches, and
    # a header that disagrees with the loader is the defect this whole mechanism exists to fix.
    if real_enabled and mock_enabled:
        decided_by = real_by if real_by == mock_by else f'{real_by}, {mock_by}'
        return ConfigSource(CONFIG_FILE, 'conflict', decided_by, None, None)

    if real_enabled:
        host = _env_str(_ENV_HOST) or ini.get("realserver", "host", fallback="localhost")
        port = _env_int(_ENV_PORT) or ini.getint("realserver", "port", fallback=8091)
        return ConfigSource(CONFIG_FILE, 'real', real_by, host, port)

    if mock_enabled:
        return ConfigSource(CONFIG_FILE, 'mock', mock_by, None, None)

    return ConfigSource(CONFIG_FILE, 'none', real_by, None, None)


def _gocaves_binary_spec(ini: ConfigParser) -> tuple[str, str, str]:
    """Which gocaves binary this run wants: path, download url, version."""
    mock_path = _env_str(_ENV_GOCAVES_PATH) or ""
    if not mock_path and ini.has_option("gocaves", "path"):
        mock_path = str(BASEDIR.joinpath(ini.get("gocaves", "path")))

    mock_url = ""
    if ini.has_option("gocaves", "url"):
        mock_url = ini.get("gocaves", "url")

    mock_version = _env_str(_ENV_GOCAVES_VERSION) or ""
    if not mock_version and ini.has_option("gocaves", "version"):
        mock_version = ini.get("gocaves", "version")

    return mock_path, mock_url, mock_version


def ensure_mock_binary() -> None:
    """Fetch the gocaves binary now, if this run needs it and does not have it.

    Called once before xdist forks.  Each worker is its own pytest session and resolves
    the mock for itself, so without this they arrive at an absent binary together and
    each pull the same 16MB.

    Does nothing when the mock is not the server this run reaches, and nothing when the
    binary is already on disk, which is every local run after the first.
    """
    ini = ConfigParser()
    ini.read(CONFIG_FILE)
    enabled, _ = _section_enabled(_ENV_GOCAVES_ENABLED, ini, "gocaves")
    if not enabled:
        return
    CavesMockServer.ensure_binary(*_gocaves_binary_spec(ini))


class CouchbaseConfig:
    def __init__(self) -> None:
        self.host: str = "localhost"
        self.port: int = 8091
        self.admin_username: str = "Administrator"
        self.admin_password: str = "password"
        self.bucket_name: str = "default"
        self.bucket_password: str = ""
        self.real_server_enabled: bool = False
        self.mock_server_enabled: bool = False
        self.mock_server: MockServer | None = None
        self.couchbase2_enabled: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_mock_server(self) -> bool:
        return self.mock_server_enabled

    @property
    def is_real_server(self) -> bool:
        return self.real_server_enabled

    @property
    def protocol(self) -> TestProtocol:
        """Which wire protocol this run targets, read off the connection-string scheme.

        The scheme is the only input, and that is what keeps the protocol gates and the
        connection agreeing: the SDK facade dispatches on the same string, so a run cannot
        be gated as one protocol and connected as another.
        """
        scheme = self.get_connection_string().split('://', 1)[0]
        if scheme == 'couchbase2':
            return TestProtocol.COUCHBASE2
        return TestProtocol.CLASSIC

    @property
    def has_management_api(self) -> bool:
        """Whether this target serves `/pools/default`, and whether its answer describes it.

        False for two unrelated reasons, which is why the three callers ask one question
        rather than repeat two conditions.  GoCAVES serves no management API.  couchbase2
        reaches the cluster through a gateway, so the only readable service list belongs to
        a deployment the run is not exercising.

        Each caller records *unknown* rather than substituting a value; see
        TestEnvironment.available_services for what unknown means downstream.
        """
        return not self.is_mock_server and self.protocol is not TestProtocol.COUCHBASE2

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def get_connection_string(self) -> str:
        if self.mock_server_enabled and self.mock_server is not None:
            return self.mock_server.connstr
        if self.couchbase2_enabled:
            return f"couchbase2://{self.host}:{self.port}"
        return f"couchbase://{self.host}"

    def get_username_and_pw(self) -> tuple[str, str]:
        return self.admin_username, self.admin_password

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        if not self.mock_server_enabled or self.mock_server is None:
            return
        self.mock_server.shutdown()

    # ------------------------------------------------------------------
    # Config loading: private helpers
    # ------------------------------------------------------------------

    @classmethod
    def _load_real_server_config(
        cls,
        config: CouchbaseConfig,
        ini: ConfigParser,
    ) -> None:
        """Populate *config* from the [realserver] section (env vars take priority)."""
        # enabled
        enabled, _ = _section_enabled(_ENV_SERVER_ENABLED, ini, "realserver")
        if not enabled:
            return

        config.real_server_enabled = True

        # couchbase2
        cb2_env = _env_bool(_ENV_COUCHBASE2)
        config.couchbase2_enabled = (
            cb2_env
            if cb2_env is not None
            else ini.getboolean("realserver", "is_couchbase2", fallback=False)
        )

        # connection details, env vars shadow ini values
        # is not None (not `or`) so a falsy-but-valid value like port 0 isn't dropped
        env_host = _env_str(_ENV_HOST)
        config.host = env_host if env_host is not None else ini.get("realserver", "host", fallback=config.host)

        env_port = _env_int(_ENV_PORT)
        config.port = env_port if env_port is not None else ini.getint("realserver", "port", fallback=config.port)

        env_username = _env_str(_ENV_USERNAME)
        config.admin_username = (
            env_username if env_username is not None
            else ini.get("realserver", "admin_username", fallback=config.admin_username)
        )

        env_password = _env_str(_ENV_PASSWORD)
        config.admin_password = (
            env_password if env_password is not None
            else ini.get("realserver", "admin_password", fallback=config.admin_password)
        )

        env_bucket = _env_str(_ENV_BUCKET)
        config.bucket_name = (
            env_bucket if env_bucket is not None
            else ini.get("realserver", "bucket_name", fallback=config.bucket_name)
        )

        config.bucket_password = ini.get("realserver", "bucket_password", fallback="")

    @classmethod
    def _load_gocaves_config(
        cls,
        config: CouchbaseConfig,
        ini: ConfigParser,
    ) -> None:
        """Populate *config* from the [gocaves] section (env vars take priority)."""
        enabled, _ = _section_enabled(_ENV_GOCAVES_ENABLED, ini, "gocaves")
        if not enabled:
            return

        if config.real_server_enabled:
            raise CouchbaseTestEnvironmentException(
                "Both real and mock servers cannot be enabled at the same time."
            )

        config.mock_server_enabled = True

        mock_path, mock_url, mock_version = _gocaves_binary_spec(ini)

        # Naming a directory is what turns the mock's log on; absent, its output is
        # discarded.  Nothing reads the file, and it is worth having only when the mock
        # itself is the thing under suspicion.
        log_dir = _env_str(_ENV_GOCAVES_LOG_DIR)
        if not log_dir and ini.has_option("gocaves", "log_dir"):
            log_dir = str(BASEDIR.joinpath(ini.get("gocaves", "log_dir")))

        config.mock_server = CouchbaseConfig.create_mock_server(
            mock_path, mock_url, mock_version, log_dir
        )
        config.bucket_name = "default"
        config.admin_username = "Administrator"
        config.admin_password = "password"

    # ------------------------------------------------------------------
    # Config loading: public entry point
    # ------------------------------------------------------------------

    @classmethod
    def load_config(cls) -> CouchbaseConfig:
        couchbase_config = cls()
        try:
            ini = ConfigParser()
            ini.read(CONFIG_FILE)

            cls._load_real_server_config(couchbase_config, ini)
            cls._load_gocaves_config(couchbase_config, ini)

        except CouchbaseTestEnvironmentException:
            raise
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(
                f"Problem trying read/load test configuration:\n{ex}"
            )

        return couchbase_config

    # ------------------------------------------------------------------
    # Mock server helpers
    # ------------------------------------------------------------------

    @staticmethod
    def create_mock_server(
        mock_path: str,
        mock_download_url: str | None,
        mock_version: str | None = None,
        log_dir: str | None = None,
    ) -> CavesMockServer:
        mock = MockServer.create_caves_mock_server(
            mock_path,
            mock_download_url,
            mock_version,
            log_dir,
        )

        try:
            mock.start()
            mock.create_cluster()
        except Exception as ex:
            raise CouchbaseTestEnvironmentException(
                f"Problem trying to start mock server:\n{ex}"
            )

        return mock

    @staticmethod
    def restart_mock(mock: MockServer) -> None:
        """Stop then restart *mock* in-place. Returns None (mock is mutated)."""
        try:
            print("\nR.I.P. mock...")
            mock.shutdown()
            time.sleep(3)
            mock.start()
        except Exception:
            import traceback
            traceback.print_exc()
            raise CouchbaseTestEnvironmentException("Error trying to restart mock")


@pytest.fixture(scope="session")
def couchbase_test_config() -> Generator[CouchbaseConfig, None, None]:
    config = CouchbaseConfig.load_config()
    yield config
    config.shutdown()
