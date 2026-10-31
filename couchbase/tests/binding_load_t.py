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

import json
import os
import pathlib
import subprocess
import sys
from typing import Any

_PACKAGE_PARENT_DIR = str(pathlib.Path(__file__).resolve().parents[2])

# Runs in a fresh interpreter, since each case needs the binding package imported for the first time.
# The finder fails `_core` once per entry of `failures`, then lets the real extension load.  Everything
# the case observes is written to stdout as one JSON document, so a stray print fails the parse.
_CHILD_SCRIPT = '''
import importlib
import importlib.abc
import json
import os
import sys

case = json.loads(sys.argv[1])
core_name = 'couchbase.protocol.classic._pycbc_core._core'
failures = list(case.get('failures', []))


class FailingFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name != core_name or not failures:
            return None
        kind = failures.pop(0)
        if kind == 'missing':
            raise ModuleNotFoundError(f'No module named {core_name!r}', name=core_name)
        raise ImportError('dlopen failed: libssl not found', name=core_name)


import ssl  # noqa: E402,F401
import couchbase  # noqa: E402

out = {'binding_loaded_by_root': any(m.startswith('couchbase.protocol.classic._pycbc_core') for m in sys.modules)}
sys.meta_path.insert(0, FailingFinder())

added_dirs = []
os.add_dll_directory = added_dirs.append
# Pinned so a case runs the same branch on every host; a real Windows interpreter would otherwise retry.
sys.platform = case.get('platform', 'linux')
if sys.platform == 'win32':
    # On a real Windows interpreter sys.path already holds a DLLs entry, which would answer before the case's.
    sys.path[:] = [p for p in sys.path if os.path.split(p)[-1] != 'DLLs']
    if case.get('dlls_path'):
        sys.path.append(case['dlls_path'])

if case.get('protocol_logger'):
    couchbase.enable_protocol_logger_to_save_network_traffic_to_file(case['protocol_logger'])
    out['captures_before_load'] = sorted(os.listdir(os.path.dirname(case['protocol_logger'])))

try:
    importlib.import_module('couchbase.protocol.classic._pycbc_core')
    out['loaded'] = True
except ImportError as exc:
    out['loaded'] = False
    out['error'] = str(exc)
    out['names_core'] = exc.name == core_name
    out['cause'] = type(exc.__cause__).__name__
out['added_dirs'] = added_dirs
print(json.dumps(out))
'''


def _run(case: dict[str, Any], **environ: str) -> dict[str, Any]:
    env = {k: v for k, v in os.environ.items() if k != 'PYCBC_OPENSSL_DIR'}
    env.update(environ)
    env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR, os.environ.get('PYTHONPATH')]))
    proc = subprocess.run([sys.executable, '-c', _CHILD_SCRIPT, json.dumps(case)],
                          env=env,
                          capture_output=True,
                          text=True,
                          timeout=120)
    assert proc.returncode == 0, proc.stderr
    result: dict[str, Any] = json.loads(proc.stdout)
    return result


class BindingLoadUnitTests:

    def test_root_does_not_load_the_binding(self) -> None:
        result = _run({})
        assert result['binding_loaded_by_root'] is False
        assert result['loaded'] is True

    def test_missing_binding(self) -> None:
        result = _run({'failures': ['missing']})
        assert result['loaded'] is False
        assert 'is not installed' in result['error']
        assert result['names_core'] is True
        assert result['cause'] == 'ModuleNotFoundError'

    def test_binding_that_fails_to_load(self) -> None:
        result = _run({'failures': ['load']})
        assert result['loaded'] is False
        assert 'could not be loaded: dlopen failed' in result['error']
        assert 'PYCBC_OPENSSL_DIR' not in result['error']
        assert result['names_core'] is True
        assert result['cause'] == 'ImportError'
        assert result['added_dirs'] == []

    def test_windows_retries_with_the_openssl_dir(self) -> None:
        result = _run({'failures': ['load'], 'platform': 'win32'}, PYCBC_OPENSSL_DIR='C:\\openssl')
        assert result['loaded'] is True
        assert result['added_dirs'] == ['C:\\openssl']

    def test_windows_retries_with_the_interpreter_dlls_dir(self) -> None:
        dlls = os.path.join('python', 'DLLs')
        result = _run({'failures': ['load'], 'platform': 'win32', 'dlls_path': dlls})
        assert result['loaded'] is True
        assert result['added_dirs'] == [dlls]

    def test_windows_retry_that_fails(self) -> None:
        result = _run({'failures': ['load', 'load'], 'platform': 'win32'}, PYCBC_OPENSSL_DIR='C:\\openssl')
        assert result['loaded'] is False
        assert 'set PYCBC_OPENSSL_DIR' in result['error']
        assert result['added_dirs'] == ['C:\\openssl']

    def test_windows_without_an_openssl_dir(self) -> None:
        result = _run({'failures': ['load'], 'platform': 'win32'})
        assert result['loaded'] is False
        assert 'set PYCBC_OPENSSL_DIR' in result['error']
        assert result['added_dirs'] == []

    def test_windows_does_not_retry_a_missing_binding(self) -> None:
        result = _run({'failures': ['missing'], 'platform': 'win32'}, PYCBC_OPENSSL_DIR='C:\\openssl')
        assert result['loaded'] is False
        assert 'is not installed' in result['error']
        assert result['added_dirs'] == []

    def test_protocol_logger_request_before_the_binding_loads(self, tmp_path: pathlib.Path) -> None:
        result = _run({'protocol_logger': str(tmp_path / 'traffic.log')})
        assert result['binding_loaded_by_root'] is False
        assert result['captures_before_load'] == []
        assert result['loaded'] is True
        assert [p.name for p in tmp_path.iterdir() if p.name.startswith('traffic.log')]
