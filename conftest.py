import pytest

pytest_plugins = [
    "tests.helpers"
]


@pytest.fixture(name="couchbase_config", scope="session")
def get_config(couchbase_test_config):
    if couchbase_test_config.mock_server_enabled:
        print("Mock server enabled!")
    if couchbase_test_config.real_server_enabled:
        print("Real server enabled!")

    return couchbase_test_config
