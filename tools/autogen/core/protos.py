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

"""The couchbase2 protocol's generated code: Protostellar at a pinned commit, through `grpcio-tools`.

Generated code refuses a protobuf runtime older than the `protoc` that wrote it, and a grpcio older
than the tools, so the tools version pinned in `protos.yaml` is the floor every user installs
against.  That pin may want a protobuf the development environment does not have, so `protoc` runs
from a venv of its own, built once into the cache and keyed by the pins.  Everything after `protoc`
is done here, in whatever interpreter runs this module.

protoc names each module from its `.proto` path, `couchbase/kv/v1/kv.proto` becoming
`couchbase.kv.v1.kv_pb2`, which would land on top of this package.  The output is moved under
`PACKAGE` and every `from couchbase.` import rewritten to match.  The service stubs, which
mypy-protobuf writes rather than protoc, use `import couchbase.kv.v1.kv_pb2` and spell each type in
full, so those imports and every qualified name are rewritten as well.  The descriptors keep their
`couchbase/...` names, which is what the server and the other SDKs call them.

`google/rpc/status.proto` is imported by the schema and never generated: `googleapis-common-protos`
ships it, and a second copy would register the same descriptor twice.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import yaml

CONFIG = Path('tools/autogen/config/protos.yaml')
OUTPUT = Path('couchbase/protocol/couchbase2/_generated')
PACKAGE = 'couchbase.protocol.couchbase2._generated'

# The schema's own top-level package, which protoc turns into the top of every module name.
SCHEMA_ROOT = 'couchbase'

_IMPORT = re.compile(rf'^from {SCHEMA_ROOT}\.([\w.]+) import (\w+)', re.MULTILINE)
_MODULE_IMPORT = re.compile(rf'^import {SCHEMA_ROOT}\.([\w.]+)$', re.MULTILINE)
_UNREWRITTEN = re.compile(rf'^\s*(from|import) {SCHEMA_ROOT}\b(?!\.protocol\.couchbase2\._generated\b)',
                          re.MULTILINE)


class ProtosError(Exception):
    pass


@dataclass(frozen=True)
class Pin:
    repository: str
    commit: str
    digest: str
    grpcio_tools: str
    googleapis_common_protos: str
    mypy_protobuf: str

    @property
    def requirements(self) -> tuple[str, ...]:
        return (f'grpcio-tools=={self.grpcio_tools}',
                f'googleapis-common-protos=={self.googleapis_common_protos}',
                f'mypy-protobuf=={self.mypy_protobuf}')


def load(root: Path) -> Pin:
    data = yaml.safe_load((root / CONFIG).read_text())
    source, generator = data['protostellar'], data['generator']
    return Pin(source['repository'], source['commit'], source['digest'],
               generator['grpcio-tools'], generator['googleapis-common-protos'], generator['mypy-protobuf'])


def cache_dir() -> Path:
    return Path(os.environ.get('XDG_CACHE_HOME') or Path.home() / '.cache') / 'pycbc-autogen' / 'protos'


def proto_files(source: Path) -> list[PurePosixPath]:
    """The schema's `.proto` files under `source`, relative to it and sorted."""
    return sorted(PurePosixPath(p.relative_to(source).as_posix())
                  for p in (source / SCHEMA_ROOT).rglob('*.proto'))


def digest(source: Path) -> str:
    """A hash of the schema's paths and contents, so a tarball GitHub recompresses still matches."""
    h = hashlib.sha256()
    for path in proto_files(source):
        h.update(str(path).encode() + b'\0' + (source / path).read_bytes() + b'\0')
    return h.hexdigest()


def _verified(source: Path, pin: Pin) -> Path:
    found = digest(source)
    if found != pin.digest:
        raise ProtosError(f'{source}: schema digest {found} is not the pinned {pin.digest}; '
                          f'is it Protostellar at {pin.commit}?')
    return source


def fetch(pin: Pin) -> Path:
    """The pinned schema, downloaded into the cache the first time it is asked for."""
    target = cache_dir() / f'protostellar-{pin.commit}'
    if target.is_dir():
        return _verified(target, pin)
    url = f'{pin.repository}/archive/{pin.commit}.tar.gz'
    with urllib.request.urlopen(url) as response:  # nosec B310: the URL is the pinned https repository
        payload = response.read()
    staging = Path(tempfile.mkdtemp(dir=_made(target.parent)))
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode='r:gz') as archive:
            for member in archive.getmembers():
                path = PurePosixPath(member.name)
                if not member.isfile() or path.suffix != '.proto' or path.parts[1:2] != (SCHEMA_ROOT,):
                    continue
                dest = staging.joinpath(*path.parts[1:])
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(archive.extractfile(member).read())
        _verified(staging, pin)
        staging.rename(target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return target


def _made(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _venv_python(venv: Path) -> Path:
    return venv / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')


def generator(pin: Pin) -> Path:
    """The interpreter of a venv holding exactly the pinned tools, built the first time it is asked for."""
    venv = cache_dir() / ('venv-' + '-'.join(r.replace('==', '-') for r in pin.requirements))
    ready = venv / 'ready'
    if ready.is_file():
        return _venv_python(venv)
    shutil.rmtree(venv, ignore_errors=True)
    subprocess.run([sys.executable, '-m', 'venv', str(_made(venv.parent) / venv.name)], check=True)
    subprocess.run([str(_venv_python(venv)), '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check',
                    *pin.requirements], check=True)
    ready.touch()
    return _venv_python(venv)


def run_protoc(python: Path, source: Path) -> dict[PurePosixPath, str]:
    """protoc's output for every schema file, by the path protoc wrote it to."""
    site = subprocess.run([str(python), '-c', 'import sysconfig; print(sysconfig.get_paths()["purelib"])'],
                          check=True, capture_output=True, text=True).stdout.strip()
    # protoc looks for a plugin on PATH, and the generator venv's scripts are not on it.
    plugin = python.with_name('protoc-gen-mypy_grpc' + ('.exe' if os.name == 'nt' else ''))
    with tempfile.TemporaryDirectory() as out:
        # `-m grpc_tools.protoc` adds the include path for the `google/protobuf` well-known types.
        subprocess.run([str(python), '-m', 'grpc_tools.protoc', f'-I{source}', f'-I{site}',
                        f'--plugin=protoc-gen-mypy_grpc={plugin}',
                        f'--python_out={out}', f'--pyi_out={out}', f'--grpc_python_out={out}',
                        f'--mypy_grpc_out=quiet:{out}', *map(str, proto_files(source))],
                       cwd=source, check=True)
        return {PurePosixPath(p.relative_to(out).as_posix()): p.read_text(encoding='utf-8')
                for p in Path(out).rglob('*') if p.is_file()}


# The year range is fixed rather than the current year, so the turn of the year regenerates nothing.
LICENCE = """#  Copyright 2016-2026. Couchbase, Inc.
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
"""

BANNER = (f'# !! AUTO-GENERATED from {CONFIG.as_posix()}: DO NOT EDIT MANUALLY !!\n'
          '# Regenerate with `python -m tools.autogen protos generate --write`.\n')


def header(pin: Pin) -> str:
    return (f'"""Protostellar {pin.commit}, generated by grpcio-tools {pin.grpcio_tools}\n'
            f'and mypy-protobuf {pin.mypy_protobuf}.\n'
            '"""\n')


def framed(text: str) -> str:
    return f'{LICENCE}\n{BANNER}' + (f'\n{text}' if text else '')


def transform(raw: dict[PurePosixPath, str], pin: Pin) -> dict[PurePosixPath, str]:
    """protoc's output as the package ships it.

    Each file is moved under `PACKAGE`, its imports rewritten, and opened with the licence and the
    generated banner; every directory gets a package file.
    """
    files: dict[PurePosixPath, str] = {}
    for path, text in raw.items():
        if path.parts[:1] != (SCHEMA_ROOT,):
            raise ProtosError(f'{path}: protoc wrote a file outside {SCHEMA_ROOT}/')
        text = _IMPORT.sub(rf'from {PACKAGE}.\1 import \2', text)
        for module in _MODULE_IMPORT.findall(text):
            text = re.sub(rf'(?<![\w.]){SCHEMA_ROOT}\.{re.escape(module)}\b', f'{PACKAGE}.{module}', text)
        files[PurePosixPath(*path.parts[1:])] = text

    modules = {'.'.join(p.with_suffix('').parts) for p in files if p.suffix == '.py'}
    for path, text in files.items():
        stray = _UNREWRITTEN.search(text)
        if stray:
            raise ProtosError(
                f'{path}: an import of {SCHEMA_ROOT} the rewrite does not cover: {stray.group(0).strip()}')
        imported = [f'{m.group(1)}.{m.group(2)}' for m in
                    re.finditer(rf'^from {re.escape(PACKAGE)}\.([\w.]+) import (\w+)', text, re.MULTILINE)]
        imported += re.findall(rf'^import {re.escape(PACKAGE)}\.([\w.]+)$', text, re.MULTILINE)
        for module in imported:
            if module not in modules:
                raise ProtosError(f'{path}: imports {module}, which is not generated')

    for path in list(files):
        for parent in list(path.parents)[:-1]:
            files.setdefault(parent / '__init__.py', '')
    files[PurePosixPath('__init__.py')] = header(pin)
    return {path: framed(text) for path, text in files.items()}


def render(root: Path, source: Path | None = None) -> dict[PurePosixPath, str]:
    pin = load(root)
    schema = _verified(source, pin) if source is not None else fetch(pin)
    return transform(run_protoc(generator(pin), schema), pin)


def tree(root: Path) -> dict[PurePosixPath, str]:
    base = root / OUTPUT
    if not base.is_dir():
        return {}
    return {PurePosixPath(p.relative_to(base).as_posix()): p.read_text(encoding='utf-8')
            for p in base.rglob('*') if p.is_file() and '__pycache__' not in p.parts}


def drift(root: Path, files: dict[PurePosixPath, str]) -> list[str]:
    """What writing `files` would change in the tree, one line per file."""
    current = tree(root)
    return ([f'  new        {OUTPUT / p}' for p in sorted(files.keys() - current.keys())]
            + [f'  changed    {OUTPUT / p}' for p in sorted(files.keys() & current.keys()) if files[p] != current[p]]
            + [f'  removed    {OUTPUT / p}' for p in sorted(current.keys() - files.keys())])


def write(root: Path, files: dict[PurePosixPath, str]) -> list[str]:
    changes = drift(root, files)
    shutil.rmtree(root / OUTPUT, ignore_errors=True)
    for path, text in files.items():
        dest = root / OUTPUT / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(text.encode('utf-8'))
    return changes
