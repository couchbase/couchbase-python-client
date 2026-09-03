#  Copyright 2016-2024. Couchbase, Inc.
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

"""pytest-asyncio runs in strict mode here: nothing sets `asyncio_mode`, `pyproject.toml`
sets only the two default loop scopes, and the plugin's own default is strict.  So every async
test in this tree carries `@pytest.mark.asyncio` and every async fixture is a
`pytest_asyncio.fixture`, hand-written and generated alike.
"""

pytest_plugins = [
    'tests_v2.hooks',
    'tests_v2.couchbase_config',
    'tests_v2.environments.test_environment',
    'tests_v2.environments.async_environment',
]
