import pathlib

import pytest

from tools.check_type_comments import (count,
                                       is_counted,
                                       measure)


@pytest.mark.parametrize('source, expected', [
    (b'x = 1  # type: int\n', 1),
    (b'def f(a):\n    # type: (int) -> None\n    pass\n', 1),
    (b'x = f()  # type: ignore[attr-defined]\n', 0),
    (b'x = f()  # type:ignore\n', 0),
    (b's = "# type: int"\n', 0),
    (b'x: int = 1\n', 0),
])
def test_count_reads_comments_not_text(source: bytes, expected: int) -> None:
    assert count(source) == expected


@pytest.mark.parametrize('path, expected', [
    ('couchbase/collection.py', True),
    ('acouchbase/management/search.py', True),
    ('couchbase/tests/query_t.py', True),
    ('tools/check_capabilities.py', False),
    ('couchbase/_core.pyi', False),
])
def test_is_counted(path: str, expected: bool) -> None:
    assert is_counted(path) is expected


def test_measure_reports_only_modules_holding_them(tmp_path: pathlib.Path) -> None:
    (tmp_path / 'a.py').write_bytes(b'x = 1  # type: int\ny = 2  # type: int\n')
    (tmp_path / 'b.py').write_bytes(b'x: int = 1\n')
    assert measure(tmp_path, ['a.py', 'b.py']) == {'a.py': 2}
