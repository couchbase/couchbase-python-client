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
import subprocess
import sys
from pathlib import PurePosixPath

import pytest

from tools.autogen.core.protos import (OUTPUT,
                                       PACKAGE,
                                       Pin,
                                       ProtosError,
                                       digest,
                                       framed,
                                       header,
                                       load,
                                       transform,
                                       tree)
from tools.check_license_headers import (HASH,
                                         is_generated,
                                         year_of)

ROOT = pathlib.Path(__file__).resolve().parents[2]

_PIN = Pin('https://example.invalid/protostellar', 'abc123', '0' * 64, '1.71.2', '1.73.1', '3.6.0')


def test_modules_move_under_the_package_and_their_imports_follow() -> None:
    files = transform({PurePosixPath('couchbase/kv/v1/kv_pb2.py'): 'from google.rpc import status_pb2\n',
                       PurePosixPath('couchbase/kv/v1/kv_pb2.pyi'): 'from couchbase.kv.v1 import kv_pb2 as _kv_pb2\n',
                       PurePosixPath('couchbase/kv/v1/kv_pb2_grpc.py'): 'from couchbase.kv.v1 import kv_pb2 as kv\n'},
                      _PIN)
    assert files[PurePosixPath('kv/v1/kv_pb2.py')] == framed('from google.rpc import status_pb2\n')
    assert files[PurePosixPath('kv/v1/kv_pb2.pyi')] == framed(f'from {PACKAGE}.kv.v1 import kv_pb2 as _kv_pb2\n')
    assert files[PurePosixPath('kv/v1/kv_pb2_grpc.py')] == framed(f'from {PACKAGE}.kv.v1 import kv_pb2 as kv\n')


def test_every_directory_is_a_package_and_the_top_records_the_pin() -> None:
    files = transform({PurePosixPath('couchbase/admin/bucket/v1/bucket_pb2.py'): ''}, _PIN)
    assert sorted(str(p) for p in files if p.name == '__init__.py') == [
        '__init__.py', 'admin/__init__.py', 'admin/bucket/__init__.py', 'admin/bucket/v1/__init__.py']
    assert files[PurePosixPath('admin/__init__.py')] == framed('')
    assert files[PurePosixPath('__init__.py')] == framed(header(_PIN))


def test_a_module_import_and_its_qualified_names_follow() -> None:
    stub = 'import couchbase.kv.v1.kv_pb2\n\ndef get(r: couchbase.kv.v1.kv_pb2.GetRequest) -> None: ...\n'
    files = transform({PurePosixPath('couchbase/kv/v1/kv_pb2.py'): '',
                       PurePosixPath('couchbase/kv/v1/kv_pb2_grpc.pyi'): stub},
                      _PIN)
    assert files[PurePosixPath('kv/v1/kv_pb2_grpc.pyi')] == framed(
        f'import {PACKAGE}.kv.v1.kv_pb2\n\ndef get(r: {PACKAGE}.kv.v1.kv_pb2.GetRequest) -> None: ...\n')


def test_an_import_the_rewrite_misses_is_refused() -> None:
    with pytest.raises(ProtosError, match='does not cover'):
        transform({PurePosixPath('couchbase/query/v1/query_pb2.py'): '    from couchbase.kv.v1 import kv_pb2\n'}, _PIN)


@pytest.mark.parametrize('text', ['from couchbase.kv.v1 import kv_pb2\n', 'import couchbase.kv.v1.kv_pb2\n'])
def test_an_import_of_a_module_not_generated_is_refused(text: str) -> None:
    with pytest.raises(ProtosError, match='not generated'):
        transform({PurePosixPath('couchbase/query/v1/query_pb2.py'): text}, _PIN)


def test_a_file_outside_the_schema_package_is_refused() -> None:
    with pytest.raises(ProtosError, match='outside'):
        transform({PurePosixPath('google/rpc/status_pb2.py'): ''}, _PIN)


def test_the_digest_covers_proto_paths_and_contents_only(tmp_path: pathlib.Path) -> None:
    proto = tmp_path / 'couchbase' / 'kv' / 'v1' / 'kv.proto'
    proto.parent.mkdir(parents=True)
    proto.write_text('syntax = "proto3";\n')
    first = digest(tmp_path)
    (tmp_path / 'README.md').write_text('not schema\n')
    assert digest(tmp_path) == first
    proto.rename(proto.with_name('kv2.proto'))
    assert digest(tmp_path) != first
    proto.with_name('kv2.proto').rename(proto)
    proto.write_text('syntax = "proto3";\n\n')
    assert digest(tmp_path) != first


def test_the_tree_records_the_configured_pin() -> None:
    assert tree(ROOT)[PurePosixPath('__init__.py')] == framed(header(load(ROOT)))


@pytest.mark.parametrize('text', ['', 'from google.rpc import status_pb2\n'])
def test_a_generated_file_passes_the_licence_check_as_generated(text: str) -> None:
    files = transform({PurePosixPath('couchbase/kv/v1/kv_pb2.py'): text}, _PIN)
    for path, generated in files.items():
        assert year_of(generated, HASH) is not None, path
        assert is_generated(generated), path


def test_every_generated_module_imports_without_a_warning() -> None:
    pytest.importorskip('grpc')
    modules = sorted('.'.join(PurePosixPath(OUTPUT.as_posix(), p).with_suffix('').parts).removesuffix('.__init__')
                     for p in tree(ROOT) if p.suffix == '.py')
    script = 'import importlib, sys\nfor m in sys.argv[1:]:\n    importlib.import_module(m)\n'
    result = subprocess.run([sys.executable, '-W', 'error', '-c', script, *modules],
                            cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
