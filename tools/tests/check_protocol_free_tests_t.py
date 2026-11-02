import pathlib
from collections import Counter

import pytest

from tools.check_protocol_free_tests import (files_read,
                                             is_read,
                                             locate,
                                             own_sites)


def measure(sources: dict[str, str]) -> dict[str, Counter[str]]:
    return {path: Counter(kind for kind, _ in sites) for path, sites in locate(sources).items()}


@pytest.mark.parametrize('source, expected', [
    ('from couchbase.protocol.classic._result import _get_from_core\n', {'classic': 1}),
    ('import couchbase.protocol.classic._error_mapping as EM\n', {'classic': 1}),
    ('from acouchbase.protocol.classic._streaming import X\n', {'classic': 1}),
    ('from couchbase.protocol.classic import _streaming\n', {'classic': 1}),
    ('from couchbase.protocol.classic._pycbc_core import _core\n', {'core': 1}),
    ('from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppViewRow\n', {'core': 1}),
    ("patch('couchbase.protocol.classic._error_mapping.ERROR_MAP')\n", {'classic': 1}),
    ("'see couchbase.protocol.classic for how'\n", {}),
    ('from couchbase.common.result import GetResult\n', {}),
    ('result._request.params\n', {'private': 1}),
    ('cb_env.cluster._impl.connection\n', {'private': 1}),
    ('rows._executor._stream\n', {'private': 2}),
    ('self._stream = stream\n', {}),
    ('result._metadata\n', {}),
])
def test_own_sites(source: str, expected: dict[str, int]) -> None:
    assert own_sites(source) == Counter(expected)


CLASSIC_ONLY_MODULE = '''
import pytest
from tests.test_features import TestProtocol
from couchbase.protocol.classic._result import _get_from_core
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
'''

CLASSIC_ONLY_MODULE_LIST = '''
import pytest
from couchbase.protocol.classic._pycbc_core import _core
pytestmark = [pytest.mark.pycbc_couchbase, pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)]
'''

CLASSIC_ONLY_CLASS = '''
from couchbase.protocol.classic._result import _get_from_core

@pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
class ClassicTests:
    def test_it(self, cb_env):
        assert cb_env.cluster._impl

class SharedTests:
    def test_it(self, cb_env):
        assert cb_env.cluster._impl
'''

CLASSIC_ONLY_CLASS_ATTRIBUTE = '''
class ClassicTests:
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]

    def test_it(self, cb_env):
        from couchbase.protocol.classic._result import _get_from_core
        assert cb_env.cluster._impl
'''

ANOTHER_PROTOCOL_ONLY = '''
from couchbase.protocol.classic._result import _get_from_core
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.CLASSIC)
'''


def test_a_classic_only_module_has_no_sites() -> None:
    assert own_sites(CLASSIC_ONLY_MODULE) == Counter()
    assert own_sites(CLASSIC_ONLY_MODULE_LIST) == Counter()


def test_a_classic_only_class_exempts_its_body_and_nothing_else() -> None:
    assert own_sites(CLASSIC_ONLY_CLASS) == Counter({'classic': 1, 'private': 1})


def test_unsupported_protocols_naming_couchbase2_exempts_the_class() -> None:
    assert own_sites(CLASSIC_ONLY_CLASS_ATTRIBUTE) == Counter()


def test_a_mark_that_does_not_name_couchbase2_exempts_nothing() -> None:
    assert own_sites(ANOTHER_PROTOCOL_ONLY) == Counter({'classic': 1})


def test_a_helper_that_imports_classic_flags_its_importers() -> None:
    counts = measure({
        'tests/classic_streams.py': 'from couchbase.protocol.classic._streaming import ClassicStream\n',
        'couchbase/tests/query_t.py': 'from tests.classic_streams import query_params\n',
        'couchbase/tests/views_t.py': 'from tests import classic_streams\n',
    })
    assert counts['couchbase/tests/query_t.py'] == Counter({'helper': 1})
    assert counts['couchbase/tests/views_t.py'] == Counter({'helper': 1})


def test_a_helper_with_only_private_reaches_does_not_flag_its_importers() -> None:
    counts = measure({
        'tests/environments/test_environment.py': 'x = cluster._impl\n',
        'couchbase/tests/kv_t.py': 'from tests.environments.test_environment import TestEnvironment\n',
    })
    assert 'couchbase/tests/kv_t.py' not in counts


def test_a_protocol_view_has_no_sites_and_only_its_package_is_free_to_import() -> None:
    counts = measure({
        'tests/protocols/__init__.py': 'from tests.protocols.classic import ClassicClusterView\n',
        'tests/protocols/classic.py': ('from couchbase.protocol.classic._cluster_impl import ClusterImpl\n'
                                       'x = cluster._impl\n'),
        'tests/environments/test_environment.py': 'from tests.protocols import view_of\n',
        'couchbase/tests/kv_t.py': 'from tests.protocols.classic import ClassicClusterView\n',
        'couchbase/tests/views_t.py': 'from tests.protocols import classic\n',
    })
    assert counts == {'couchbase/tests/kv_t.py': Counter({'helper': 1}),
                      'couchbase/tests/views_t.py': Counter({'helper': 1})}


def test_locate_gives_each_site_its_line() -> None:
    found = locate({
        'tests/classic_streams.py': 'import os\nfrom couchbase.protocol.classic._streaming import ClassicStream\n',
        'couchbase/tests/query_t.py': ('from tests.classic_streams import query_params\n\n'
                                       'def test_it(result):\n    assert result._request\n'),
    })
    assert found == {'tests/classic_streams.py': [('classic', 2)],
                     'couchbase/tests/query_t.py': [('helper', 1), ('private', 4)]}


@pytest.mark.parametrize('path, expected', [
    ('couchbase/tests/query_t.py', True),
    ('tests/classic_streams.py', True),
    ('acouchbase/tests/_generated/query_t.py', False),
    ('tests/config.yaml', False),
])
def test_is_read(path: str, expected: bool) -> None:
    assert is_read(path) is expected


def test_the_tree_has_no_sites() -> None:
    root = pathlib.Path(__file__).resolve().parents[2]
    assert locate({p: (root / p).read_text() for p in files_read(root)}) == {}
