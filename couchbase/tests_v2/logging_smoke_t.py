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

import logging
import sys
from collections.abc import Generator
from typing import ClassVar

import pytest

from tests_v2.manifest import validate_test_manifest


class LoggingSmokeTestSuite:
    # No cb_env: every test imports a module or reads a logger name.  Not generated, because
    # none of what it asserts differs per api.
    #
    # The shape it holds in place: the four logging callables are importable from `couchbase`,
    # the loggers sit under a single `couchbase` parent, and importing the pieces in either
    # order works.
    #
    # The imports are bare on purpose.  Wrapping one in `try: ... except ImportError:
    # pytest.fail(...)` changes nothing about what fails, and replaces the traceback naming the
    # module that actually failed with a one-line message.

    TEST_MANIFEST: ClassVar[list[str]] = [
        'test_backward_compat_imports',
        'test_configure_logging_callable',
        'test_enable_protocol_logger_callable',
        'test_get_metadata_callable',
        'test_get_transactions_protocol_callable',
        'test_import_core_metadata_module',
        'test_import_logging_config_module',
        'test_logger_hierarchy',
        'test_logger_names_correct',
        'test_no_circular_imports',
    ]

    def test_import_logging_config_module(self) -> None:
        import couchbase.logic.logging_config
        assert couchbase.logic.logging_config is not None

    def test_import_core_metadata_module(self) -> None:
        from couchbase.logic.pycbc_core import core_metadata
        assert core_metadata is not None

    def test_configure_logging_callable(self) -> None:
        from couchbase import configure_logging
        assert callable(configure_logging)

    def test_enable_protocol_logger_callable(self) -> None:
        from couchbase import enable_protocol_logger_to_save_network_traffic_to_file
        assert callable(enable_protocol_logger_to_save_network_traffic_to_file)

    def test_get_metadata_callable(self) -> None:
        from couchbase import get_metadata
        assert callable(get_metadata)

        metadata = get_metadata()
        assert isinstance(metadata, dict)
        assert len(metadata) > 0

    def test_get_transactions_protocol_callable(self) -> None:
        from couchbase import get_transactions_protocol
        assert callable(get_transactions_protocol)

        result = get_transactions_protocol()
        assert result is None or isinstance(result, tuple)

    def test_logger_names_correct(self) -> None:
        assert logging.getLogger('couchbase.threshold').name == 'couchbase.threshold'
        assert logging.getLogger('couchbase.metrics').name == 'couchbase.metrics'
        assert logging.getLogger('couchbase.transactions').name == 'couchbase.transactions'

    def test_logger_hierarchy(self) -> None:
        parent = logging.getLogger('couchbase')
        threshold_logger = logging.getLogger('couchbase.threshold')
        metrics_logger = logging.getLogger('couchbase.metrics')

        # The parent identity is what a caller relies on when it configures a handler on
        # 'couchbase' and expects both child loggers to propagate to it.  Comparing names is
        # not enough for that: a logger whose name merely starts with 'couchbase.' would pass
        # while propagating somewhere else entirely.
        assert threshold_logger.parent is parent
        assert metrics_logger.parent is parent

    def test_no_circular_imports(self) -> None:
        import couchbase
        assert couchbase is not None

        # Force a fresh execution of each module rather than trusting the already-imported
        # one, since a cycle only shows up while the module body is running.
        #
        # Both entries are restored afterwards.  A re-import binds a *new* module object to
        # sys.modules while every module that did `from ... import X` keeps the old one, so
        # leaving the new object in place hands a split identity to whatever runs next.
        fresh_imports = ['couchbase.logic.logging_config',
                         'couchbase.logic.pycbc_core.core_metadata']
        saved = {name: sys.modules[name] for name in fresh_imports if name in sys.modules}
        try:
            for name in fresh_imports:
                sys.modules.pop(name, None)

            import couchbase.logic.logging_config
            assert couchbase.logic.logging_config is not None

            from couchbase.logic.pycbc_core import core_metadata
            assert core_metadata is not None
        finally:
            for name in fresh_imports:
                sys.modules.pop(name, None)
            sys.modules.update(saved)

    def test_backward_compat_imports(self) -> None:
        # The four names below were defined in couchbase/__init__.py before the logging move.
        # Importing them from `couchbase` has to keep working regardless of where they now live.
        from couchbase import (configure_logging,
                               enable_protocol_logger_to_save_network_traffic_to_file,
                               get_metadata,
                               get_transactions_protocol)

        assert callable(configure_logging)
        assert callable(enable_protocol_logger_to_save_network_traffic_to_file)
        assert callable(get_metadata)
        assert callable(get_transactions_protocol)

        assert isinstance(get_metadata(), dict)


class LoggingSmokeTests(LoggingSmokeTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(LoggingSmokeTestSuite, self.__class__)

    @pytest.fixture(autouse=True)
    def restore_logger_state(self) -> Generator[None, None, None]:
        # getLogger() creates and permanently registers a logger the first time it is asked
        # for one, so simply reading the three names above mutates process-wide state.  The
        # levels and handlers are restored so a suite that runs after this one sees whatever
        # the session had configured, not what these tests left.
        names = ['couchbase', 'couchbase.threshold', 'couchbase.metrics', 'couchbase.transactions']
        saved = [(logging.getLogger(n), logging.getLogger(n).level, list(logging.getLogger(n).handlers))
                 for n in names]
        yield
        for logger, level, handlers in saved:
            logger.setLevel(level)
            logger.handlers[:] = handlers
