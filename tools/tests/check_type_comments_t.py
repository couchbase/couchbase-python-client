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
