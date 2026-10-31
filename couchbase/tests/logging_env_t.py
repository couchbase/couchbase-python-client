#  Copyright 2016-2026. Couchbase, Inc.
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

import io
import logging
import pathlib
from collections.abc import Generator
from typing import cast

import pytest

from couchbase.common._logging import (TRACE,
                                       CoreLogRouter,
                                       EnvLogging,
                                       configure_python_logging,
                                       read_env_logging)
from couchbase.common.exceptions import InvalidArgumentException

_LOGGER_NAME = 'couchbase_logging_env_t'


class _RecordingBackend:

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def apply_env(self, env: EnvLogging) -> None:
        self.calls.append(('apply_env', env))

    def route_to(self, logger: logging.Logger, level: int) -> None:
        self.calls.append(('route_to', (logger.name, level)))

    def enable_protocol_logger(self, filename: str) -> None:
        self.calls.append(('enable_protocol_logger', filename))


def _env(**environ: str) -> EnvLogging:
    env, _ = read_env_logging(environ)
    assert env is not None
    return env


def _clear_root_handlers() -> None:
    # pytest's log capture adds its handlers to root after the fixtures have run.
    logging.getLogger().handlers.clear()


def _capture(logger: logging.Logger) -> io.StringIO:
    stream = io.StringIO()
    logger.addHandler(logging.StreamHandler(stream))
    return stream


class LoggingEnvUnitTests:

    @pytest.fixture(autouse=True)
    def isolated_loggers(self) -> Generator[None, None, None]:
        # Root and the SDK logger are process-wide; every test here leaves them as it found them.
        root = logging.getLogger()
        sdk_logger = logging.getLogger('couchbase')
        test_logger = logging.getLogger(_LOGGER_NAME)
        saved_root = list(root.handlers)
        saved_sdk = (sdk_logger.level, sdk_logger.propagate, list(sdk_logger.handlers))
        yield
        for handler in test_logger.handlers:
            handler.close()
        test_logger.handlers.clear()
        test_logger.setLevel(logging.NOTSET)
        test_logger.propagate = True
        root.handlers[:] = saved_root
        sdk_logger.level, sdk_logger.propagate = saved_sdk[0], saved_sdk[1]
        sdk_logger.handlers[:] = saved_sdk[2]

    def test_no_level_means_no_logging(self) -> None:
        assert read_env_logging({'PYCBC_LOG_FILE': 'sdk.log'}) == (None, None)

    @pytest.mark.parametrize('raw, level, python_level', [
        ('trace', 'trace', TRACE),
        ('DEBUG', 'debug', logging.DEBUG),
        (' warn ', 'warning', logging.WARNING),
        ('warning', 'warning', logging.WARNING),
        ('off', 'off', logging.CRITICAL + 10),
    ])
    def test_level_names_agree_on_both_sides(self, raw: str, level: str, python_level: int) -> None:
        env = _env(PYCBC_LOG_LEVEL=raw)
        assert (env.level, env.python_level) == (level, python_level)

    def test_unrecognized_level_is_info_on_both_sides(self) -> None:
        env, unrecognized = read_env_logging({'PYCBC_LOG_LEVEL': 'verbose'})
        assert env is not None
        assert (env.level, env.python_level, unrecognized) == ('info', logging.INFO, 'verbose')

    def test_unrecognized_level_warns(self) -> None:
        stream = _capture(logging.getLogger(_LOGGER_NAME))
        env, unrecognized = read_env_logging({'PYCBC_LOG_LEVEL': 'verbose'})
        assert env is not None
        configure_python_logging(env, unrecognized, logger_name=_LOGGER_NAME)
        assert "Unrecognized PYCBC_LOG_LEVEL value 'verbose', defaulting to INFO" in stream.getvalue()

    @pytest.mark.parametrize('value, enabled', [
        (None, False),
        ('0', False),
        ('false', False),
        ('1', True),
        ('true', True),
    ])
    def test_enable_console_reads_the_value(self, value: str | None, enabled: bool) -> None:
        environ = {'PYCBC_LOG_LEVEL': 'info', 'PYCBC_LOG_FILE': 'sdk.log'}
        if value is not None:
            environ['PYCBC_ENABLE_CONSOLE'] = value
        assert _env(**environ).enable_console is enabled

    def test_console_without_a_file(self) -> None:
        assert _env(PYCBC_LOG_LEVEL='info', PYCBC_ENABLE_CONSOLE='0').enable_console is True

    def test_root_handlers_receive_sdk_records(self) -> None:
        _clear_root_handlers()
        app_stream = _capture(logging.getLogger())
        configure_python_logging(_env(PYCBC_LOG_LEVEL='debug'), logger_name=_LOGGER_NAME)
        logger = logging.getLogger(_LOGGER_NAME)
        logger.debug('sdk record')
        assert logger.level == logging.DEBUG
        assert logger.handlers == []
        assert logger.propagate is True
        assert 'sdk record' in app_stream.getvalue()

    def test_application_handler_on_sdk_logger_is_kept(self) -> None:
        _clear_root_handlers()
        logger = logging.getLogger(_LOGGER_NAME)
        app_stream = _capture(logger)
        app_handler = logger.handlers[0]
        configure_python_logging(_env(PYCBC_LOG_LEVEL='info'), logger_name=_LOGGER_NAME)
        logger.info('sdk record')
        assert logger.handlers == [app_handler]
        assert logger.propagate is True
        assert 'sdk record' in app_stream.getvalue()

    def test_console_handler_only_when_nothing_else_handles(self) -> None:
        _clear_root_handlers()
        configure_python_logging(_env(PYCBC_LOG_LEVEL='info'), logger_name=_LOGGER_NAME)
        logger = logging.getLogger(_LOGGER_NAME)
        assert [type(h) for h in logger.handlers] == [logging.StreamHandler]
        assert logger.propagate is False

    def test_file_without_console(self, tmp_path: pathlib.Path) -> None:
        _clear_root_handlers()
        log_file = tmp_path / 'sdk.log'
        configure_python_logging(_env(PYCBC_LOG_LEVEL='info', PYCBC_LOG_FILE=str(log_file), PYCBC_ENABLE_CONSOLE='0'),
                                 logger_name=_LOGGER_NAME)
        logger = logging.getLogger(_LOGGER_NAME)
        logger.info('sdk record')
        assert [type(h) for h in logger.handlers] == [logging.FileHandler]
        assert 'sdk record' in log_file.read_text()

    def test_file_with_console(self, tmp_path: pathlib.Path) -> None:
        _clear_root_handlers()
        configure_python_logging(_env(PYCBC_LOG_LEVEL='info', PYCBC_LOG_FILE=str(tmp_path / 'sdk.log'),
                                      PYCBC_ENABLE_CONSOLE='1'),
                                 logger_name=_LOGGER_NAME)
        logger = logging.getLogger(_LOGGER_NAME)
        assert [type(h) for h in logger.handlers] == [logging.FileHandler, logging.StreamHandler]

    def test_file_is_written_even_when_the_application_handles(self, tmp_path: pathlib.Path) -> None:
        _clear_root_handlers()
        app_stream = _capture(logging.getLogger())
        log_file = tmp_path / 'sdk.log'
        configure_python_logging(_env(PYCBC_LOG_LEVEL='info', PYCBC_LOG_FILE=str(log_file), PYCBC_ENABLE_CONSOLE='1'),
                                 logger_name=_LOGGER_NAME)
        logger = logging.getLogger(_LOGGER_NAME)
        logger.info('sdk record')
        assert [type(h) for h in logger.handlers] == [logging.FileHandler]
        assert 'sdk record' in log_file.read_text()
        assert 'sdk record' in app_stream.getvalue()

    def test_logging_module_is_not_patched(self) -> None:
        assert logging.getLevelName(TRACE) == 'TRACE'
        assert not hasattr(logging, 'TRACE')
        assert not hasattr(logging.Logger, 'trace')

    def test_env_is_applied_when_the_backend_registers(self) -> None:
        env = _env(PYCBC_LOG_LEVEL='debug')
        router = CoreLogRouter(env)
        backend = _RecordingBackend()
        router.register(backend)
        assert backend.calls == [('apply_env', env)]

    def test_configure_logging_conflicts_with_env(self) -> None:
        router = CoreLogRouter(_env(PYCBC_LOG_LEVEL='debug'))
        with pytest.raises(RuntimeError, match='PYCBC_LOG_LEVEL'):
            router.route_to(logging.getLogger(_LOGGER_NAME), logging.INFO)

    def test_configure_logging_is_held_until_the_backend_registers(self) -> None:
        router = CoreLogRouter(None)
        router.route_to(logging.getLogger(_LOGGER_NAME), logging.DEBUG)
        backend = _RecordingBackend()
        router.register(backend)
        assert backend.calls == [('route_to', (_LOGGER_NAME, logging.DEBUG))]

    def test_configure_logging_twice_before_the_backend_registers(self) -> None:
        router = CoreLogRouter(None)
        router.route_to(logging.getLogger(_LOGGER_NAME), logging.DEBUG)
        with pytest.raises(RuntimeError, match='already been called'):
            router.route_to(logging.getLogger(_LOGGER_NAME), logging.DEBUG)

    def test_configure_logging_after_the_backend_registers(self) -> None:
        router = CoreLogRouter(None)
        backend = _RecordingBackend()
        router.register(backend)
        router.route_to(logging.getLogger(_LOGGER_NAME), logging.WARNING)
        assert backend.calls == [('route_to', (_LOGGER_NAME, logging.WARNING))]

    def test_protocol_logger_is_held_until_the_backend_registers(self) -> None:
        stream = _capture(logging.getLogger('couchbase'))
        router = CoreLogRouter(None)
        router.enable_protocol_logger('first.log')
        router.enable_protocol_logger('traffic.log')
        backend = _RecordingBackend()
        router.register(backend)
        assert backend.calls == [('enable_protocol_logger', 'traffic.log')]
        assert stream.getvalue() == ''

    @pytest.mark.parametrize('filename', ['', 'traffic\0.log', None, b'traffic.log'])
    def test_protocol_logger_rejects_a_bad_filename_at_the_call(self, filename: object) -> None:
        router = CoreLogRouter(None)
        with pytest.raises(InvalidArgumentException):
            router.enable_protocol_logger(cast(str, filename))
        backend = _RecordingBackend()
        router.register(backend)
        assert backend.calls == []

    def test_protocol_without_core_warns_of_a_held_request(self) -> None:
        stream = _capture(logging.getLogger('couchbase'))
        router = CoreLogRouter(None)
        router.enable_protocol_logger('traffic.log')
        router.register_protocol_without_core('couchbase2')
        assert 'The couchbase2 protocol has no protocol logger' in stream.getvalue()

    def test_request_after_a_protocol_without_core_warns(self) -> None:
        stream = _capture(logging.getLogger('couchbase'))
        router = CoreLogRouter(None)
        router.register_protocol_without_core('couchbase2')
        assert stream.getvalue() == ''
        router.enable_protocol_logger('traffic.log')
        assert 'The couchbase2 protocol has no protocol logger' in stream.getvalue()

    def test_protocol_logger_with_a_backend(self) -> None:
        router = CoreLogRouter(None)
        backend = _RecordingBackend()
        router.register(backend)
        router.enable_protocol_logger('traffic.log')
        assert backend.calls == [('enable_protocol_logger', 'traffic.log')]

    @pytest.mark.parametrize('failing_call', ['apply_env', 'enable_protocol_logger'])
    def test_registration_that_raises_is_retried(self, failing_call: str) -> None:
        env = _env(PYCBC_LOG_LEVEL='debug') if failing_call == 'apply_env' else None
        router = CoreLogRouter(env)
        router.enable_protocol_logger('traffic.log')
        failing = _RecordingBackend()

        def fail(*args: object) -> None:
            raise RuntimeError('backend failed')

        setattr(failing, failing_call, fail)
        with pytest.raises(RuntimeError, match='backend failed'):
            router.register(failing)
        backend = _RecordingBackend()
        router.register(backend)
        expected: list[tuple[str, object]] = [('enable_protocol_logger', 'traffic.log')]
        if env is not None:
            expected.insert(0, ('apply_env', env))
        assert backend.calls == expected

    def test_first_backend_wins(self) -> None:
        router = CoreLogRouter(None)
        first, second = _RecordingBackend(), _RecordingBackend()
        router.register(first)
        router.register(second)
        router.enable_protocol_logger('traffic.log')
        assert first.calls == [('enable_protocol_logger', 'traffic.log')]
        assert second.calls == []
