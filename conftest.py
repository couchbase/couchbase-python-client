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

import pytest

pytest_plugins = [
    'tests.hooks',
    'tests.couchbase_config',
    'tests.environments.test_environment',
    'tests.environments.async_environment',
    'tests.environments.twisted_environment',
]


def pytest_addoption(parser):
    parser.addoption(
        "--txcouchbase", action="store_true", default=False, help="run txcouchbase tests"
    )
    # Declare which server the run is meant to reach, so a config that says otherwise aborts
    # instead of quietly running somewhere else.  Enforced in tests/hooks.py, which is
    # where the config being asserted about lives; registered here because this is an initial
    # conftest and so is guaranteed to have added the flags before the command line is parsed.
    parser.addoption(
        "--real-server", action="store_true", default=False,
        help="assert this run reaches a real cluster; fails if the config selects the mock"
    )
    parser.addoption(
        "--mock", action="store_true", default=False,
        help="assert this run reaches the GoCAVES mock; fails if the config selects a real cluster"
    )


_API_MARKERS = {
    'couchbase': pytest.mark.pycbc_couchbase,
    'acouchbase': pytest.mark.pycbc_acouchbase,
    'txcouchbase': pytest.mark.pycbc_txcouchbase,
}


def pytest_collection_modifyitems(items):
    for item in items:
        marker = _API_MARKERS.get(item.nodeid.split('/', 1)[0])
        if marker is not None:
            item.add_marker(marker)
