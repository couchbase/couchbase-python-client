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


import logging
import sys

import pytest


class LoggingSmokeTestSuite:
    TEST_MANIFEST = [
        'test_import_logging_config_module',
        'test_import_core_metadata_module',
        'test_configure_logging_callable',
        'test_enable_protocol_logger_callable',
        'test_get_metadata_callable',
        'test_get_transactions_protocol_callable',
        'test_logger_names_correct',
        'test_logger_hierarchy',
        'test_no_circular_imports',
        'test_backward_compat_imports',
    ]

    def test_import_logging_config_module(self):
        """Importing couchbase._logging_config should succeed."""
        try:
            import couchbase.logic.logging_config
            assert couchbase.logic.logging_config is not None
        except ImportError as e:
            pytest.fail(f"Failed to import couchbase._logging_config: {e}")

    def test_import_core_metadata_module(self):
        """Importing core_metadata should succeed."""
        try:
            from couchbase.logic.pycbc_core import core_metadata
            assert core_metadata is not None
        except ImportError as e:
            pytest.fail(f"Failed to import core_metadata: {e}")

    def test_configure_logging_callable(self):
        """configure_logging should be importable from couchbase and callable."""
        try:
            from couchbase import configure_logging
            assert callable(configure_logging)
        except ImportError as e:
            pytest.fail(f"Failed to import configure_logging: {e}")

    def test_enable_protocol_logger_callable(self):
        """enable_protocol_logger_to_save_network_traffic_to_file should be importable and callable."""
        try:
            from couchbase import enable_protocol_logger_to_save_network_traffic_to_file
            assert callable(enable_protocol_logger_to_save_network_traffic_to_file)
        except ImportError as e:
            pytest.fail(f"Failed to import enable_protocol_logger_to_save_network_traffic_to_file: {e}")

    def test_get_metadata_callable(self):
        """get_metadata should be importable from couchbase and callable."""
        try:
            from couchbase import get_metadata
            assert callable(get_metadata)

            # Verify it actually works
            metadata = get_metadata()
            assert isinstance(metadata, dict)
            assert 'version' in metadata or len(metadata) > 0
        except ImportError as e:
            pytest.fail(f"Failed to import get_metadata: {e}")

    def test_get_transactions_protocol_callable(self):
        """get_transactions_protocol should be importable from couchbase and callable."""
        try:
            from couchbase import get_transactions_protocol
            assert callable(get_transactions_protocol)

            # Verify it returns expected types (tuple or None)
            result = get_transactions_protocol()
            assert result is None or isinstance(result, tuple)
        except ImportError as e:
            pytest.fail(f"Failed to import get_transactions_protocol: {e}")

    def test_logger_names_correct(self):
        """SDK loggers should have correct names."""
        # Get loggers (doesn't require configuration)
        threshold_logger = logging.getLogger('couchbase.threshold')
        metrics_logger = logging.getLogger('couchbase.metrics')
        transactions_logger = logging.getLogger('couchbase.transactions')

        # Verify logger names
        assert threshold_logger.name == 'couchbase.threshold'
        assert metrics_logger.name == 'couchbase.metrics'
        assert transactions_logger.name == 'couchbase.transactions'

    def test_logger_hierarchy(self):
        """Python SDK loggers should be children of 'couchbase' parent logger."""
        parent_logger = logging.getLogger('couchbase')
        threshold_logger = logging.getLogger('couchbase.threshold')
        metrics_logger = logging.getLogger('couchbase.metrics')

        # Verify hierarchy (child names start with parent name)
        assert threshold_logger.name.startswith(parent_logger.name)
        assert metrics_logger.name.startswith(parent_logger.name)

        # Verify parent is actually the parent
        assert threshold_logger.parent == parent_logger or threshold_logger.parent.name == parent_logger.name
        assert metrics_logger.parent == parent_logger or metrics_logger.parent.name == parent_logger.name

    def test_no_circular_imports(self):
        """Importing modules in various orders should not cause circular import errors."""
        # Test 1: Import main module
        try:
            import couchbase
            assert couchbase is not None
        except ImportError as e:
            pytest.fail(f"Circular import detected when importing couchbase: {e}")

        # Test 2: Import logging_config first
        try:
            # Reload to test fresh import
            if 'couchbase.logic.logging_config' in sys.modules:
                del sys.modules['couchbase.logic.logging_config']
            import couchbase.logic.logging_config
            assert couchbase.logic.logging_config is not None
        except ImportError as e:
            pytest.fail(f"Circular import detected when importing couchbase.logic.logging_config: {e}")

        # Test 3: Import core_metadata first
        try:
            # Reload to test fresh import
            if 'couchbase.logic.pycbc_core.core_metadata' in sys.modules:
                del sys.modules['couchbase.logic.pycbc_core.core_metadata']
            from couchbase.logic.pycbc_core import core_metadata
            assert core_metadata is not None
        except ImportError as e:
            pytest.fail(f"Circular import detected when importing core_metadata: {e}")

    def test_backward_compat_imports(self):
        """All public API imports from couchbase should still work."""
        # These were previously defined in __init__.py
        # After refactor, they should still be importable from couchbase

        try:
            from couchbase import (configure_logging,
                                   enable_protocol_logger_to_save_network_traffic_to_file,
                                   get_metadata,
                                   get_transactions_protocol)

            # Verify they're all callable
            assert callable(configure_logging)
            assert callable(enable_protocol_logger_to_save_network_traffic_to_file)
            assert callable(get_metadata)
            assert callable(get_transactions_protocol)

            # Verify get_metadata actually works
            metadata = get_metadata()
            assert isinstance(metadata, dict)

        except ImportError as e:
            pytest.fail(f"Backward compatibility broken - import failed: {e}")
        except Exception as e:
            pytest.fail(f"Backward compatibility broken - function call failed: {e}")


class ClassicLoggingSmokeTests(LoggingSmokeTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicLoggingSmokeTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicLoggingSmokeTests) if valid_test_method(meth)]
        test_list = set(LoggingSmokeTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated. Missing/extra tests: {test_list}.')
