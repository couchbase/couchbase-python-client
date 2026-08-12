#  Copyright 2016-2023. Couchbase, Inc.
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

import os
import platform
import shutil
import subprocess  # nosec
import sys
from dataclasses import dataclass, field
from sysconfig import get_config_var
from typing import (Dict,
                    List,
                    Optional)

from setuptools import Command, Extension

# need at least setuptools v62.3.0
from setuptools.command.build import build
from setuptools.command.build_ext import build_ext
from setuptools.errors import OptionError, SetupError

CMAKE_EXE = os.environ.get('CMAKE_EXE', shutil.which('cmake'))
PYCBC_ROOT = os.path.dirname(__file__)
# PYCBC_CXXCBC_CACHE_DIR should only need to be used on Windows when setting the CPM cache (PYCBC_SET_CPM_CACHE=ON).
# It helps prevent issues w/ path lengths.
# NOTE: Setting the CPM cache on a Windows machine should be a _rare_ occasion.  When doing so and setting
# PYCBC_CXXCBC_CACHE_DIR, be sure to copy the cache to <root source dir>\deps\couchbase-cxx-cache if building a sdist.
CXXCBC_CACHE_DIR = os.environ.get('PYCBC_CXXCBC_CACHE_DIR', os.path.join(PYCBC_ROOT, 'deps', 'couchbase-cxx-cache'))
ENV_TRUE = ['true', '1', 'y', 'yes', 'on']


def use_py_limited_api() -> bool:
    """Return True if the extension should be built against Py_LIMITED_API (abi3).

    Defaults to True so we publish a single stable-ABI wheel per platform.
    Power users can disable this by exporting ``PYCBC_PY_LIMITED_API=false``
    (or 0/no/off) to build a CPython-version-specific binary.
    """
    return os.getenv('PYCBC_PY_LIMITED_API', 'true').lower() in ENV_TRUE


# Lowest CPython version pycbc's C extension supports under the stable ABI.
# Raising this floor frees up symbols (e.g. Py_T_OBJECT_EX is limited-API only
# from 3.12), but invalidates wheels for older CPython releases.
DEFAULT_PY_LIMITED_API_VERSION = '3.10'


def _parse_py_limited_api_version(value: str) -> tuple:
    try:
        major_str, minor_str = value.split('.', 1)
        major, minor = int(major_str), int(minor_str)
    except (ValueError, AttributeError) as e:
        raise OptionError(
            f'PYCBC_PY_LIMITED_API_VERSION must be in MAJOR.MINOR form (e.g. "3.10"); got {value!r}'
        ) from e
    if major != 3 or minor < 10:
        raise OptionError(
            f'PYCBC_PY_LIMITED_API_VERSION must be 3.10 or newer; got {value!r}. '
            'pycbc relies on stable-ABI symbols (PyType_FromSpec, PyUnicode_AsUTF8, ...) '
            'first exposed in 3.10.'
        )
    return major, minor


def py_limited_api_version() -> tuple:
    """Return the (major, minor) CPython version that bounds the stable-ABI build."""
    return _parse_py_limited_api_version(
        os.getenv('PYCBC_PY_LIMITED_API_VERSION', DEFAULT_PY_LIMITED_API_VERSION))


def py_limited_api_hex() -> str:
    """Return the Py_LIMITED_API hex literal (e.g. '0x030A0000') passed to the C compiler."""
    major, minor = py_limited_api_version()
    return f'0x{major:02X}{minor:02X}0000'


def py_limited_api_wheel_tag() -> str:
    """Return the bdist_wheel --py-limited-api tag (e.g. 'cp310')."""
    major, minor = py_limited_api_version()
    return f'cp{major}{minor}'


def check_for_cmake():
    if not CMAKE_EXE:
        print('cmake executable not found. '
              'Set CMAKE_EXE environment or update your path')
        sys.exit(1)


def process_build_env_vars():  # noqa: C901
    # Set debug or release
    build_type = os.getenv('PYCBC_BUILD_TYPE', 'Release')
    if build_type == 'Debug':
        # @TODO: extra Windows debug args?
        if platform.system() != "Windows":
            debug_flags = ' '.join(['-O0', '-g3'])
            c_flags = os.getenv('CFLAGS', '')
            cxx_flags = os.getenv('CXXFLAGS', '')
            os.environ['CFLAGS'] = f'{c_flags} {debug_flags}'
            os.environ['CXXFLAGS'] = f'{cxx_flags} {debug_flags}'
    os.environ['PYCBC_BUILD_TYPE'] = build_type
    cmake_extra_args = []

    # Allows us to set the location of OpenSSL for the build.
    ssl_dir = os.getenv('PYCBC_OPENSSL_DIR', None)
    if ssl_dir is not None:
        cmake_extra_args += [f'-DOPENSSL_ROOT_DIR={ssl_dir}']

    # We use OpenSSL by default if building the SDK; however, starting with v4.1.9 we build our wheels using BoringSSL.
    pycbc_use_openssl = os.getenv('PYCBC_USE_OPENSSL', 'true').lower() in ENV_TRUE
    pycbc_use_opensslv1_1 = os.getenv('PYCBC_USE_OPENSSLV1_1', 'false').lower() in ENV_TRUE
    if pycbc_use_openssl is True or pycbc_use_opensslv1_1:
        cmake_extra_args += ['-DUSE_STATIC_BORINGSSL:BOOL=OFF']
        ssl_version = os.getenv('PYCBC_OPENSSL_VERSION', None)
        if pycbc_use_opensslv1_1 is True:
            cmake_extra_args += ['-DUSE_OPENSSLV1_1:BOOL=ON']
            if not ssl_version:
                # lastest 1.1 version: https://github.com/openssl/openssl/releases/tag/OpenSSL_1_1_1w
                ssl_version = '1.1.1w'
        elif not ssl_version:
            # lastest 3.x version: https://github.com/openssl/openssl/releases/tag/openssl-3.5.2
            ssl_version = '3.5.2'
        cmake_extra_args += [f'-DOPENSSL_VERSION={ssl_version}']

    else:
        cmake_extra_args += ['-DUSE_STATIC_BORINGSSL:BOOL=ON']

    # v4.1.9: building with static stdlibc++ must be opted-in by user
    use_static_stdlib = os.getenv('PYCBC_USE_STATIC_STDLIB', 'false').lower() in ENV_TRUE
    if use_static_stdlib is True:
        cmake_extra_args += ['-DUSE_STATIC_STDLIB:BOOL=ON']
    else:
        cmake_extra_args += ['-DUSE_STATIC_STDLIB:BOOL=OFF']

    # v4.3.4: Allow user to specify if the C++ core will download Mozilla CA bundle during build.
    #         Defaults to ON unless the CPM Cache is being used then we use the certs from the cache
    download_mozilla_ca_bundle = os.getenv('PYCBC_DOWNLOAD_MOZILLA_CA_BUNDLE', None)
    if download_mozilla_ca_bundle is not None:
        if download_mozilla_ca_bundle.lower() in ENV_TRUE:
            cmake_extra_args += ['-DDOWNLOAD_MOZILLA_CA_BUNDLE:BOOL=ON']
        else:
            cmake_extra_args += ['-DDOWNLOAD_MOZILLA_CA_BUNDLE:BOOL=OFF']

    sanitizers = os.getenv('PYCBC_SANITIZERS', None)
    if sanitizers:
        for x in sanitizers.split(','):
            cmake_extra_args += [f'-DENABLE_SANITIZER_{x.upper()}=ON']

    if os.getenv('PYCBC_VERBOSE_MAKEFILE', None):
        cmake_extra_args += ['-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON']

    pycbc_cmake_system_version = os.getenv('PYCBC_CMAKE_SYSTEM_VERSION', None)
    if pycbc_cmake_system_version is not None:
        cmake_extra_args += [f'-DCMAKE_SYSTEM_VERSION={pycbc_cmake_system_version}']

    pycbc_tls_key_log_file = os.getenv('PYCBC_TLS_KEY_LOG_FILE', None)
    if pycbc_tls_key_log_file is not None:
        cmake_extra_args += [f'-DCOUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE={pycbc_tls_key_log_file}']

    # Hand CMake this interpreter's own module suffix (e.g. .cpython-310-darwin.so)
    # so it stays the single source of truth for both build modes: a version-specific
    # build uses it as-is, while a stable-ABI build keeps only its trailing .so/.pyd
    # (pairing the .so with ".abi3"), matching CMakeBuildExt.get_ext_filename() below.
    # Without the interpreter tag, a version-specific build would produce a bare
    # "_core.so" that any interpreter's import machinery will load, defeating the
    # point of opting out of the stable ABI.
    cmake_extra_args += [f'-DPYCBC_C_MOD_SUFFIX={get_config_var("EXT_SUFFIX")}']

    # PYCBC_PY_LIMITED_API: build against the Python stable ABI (abi3).
    # Defaults to ON; opt out for a Python-version-specific binary.
    # The hex value (e.g. 0x030A0000 == 3.10) is the lowest CPython the resulting
    # binary will load against, and must match the wheel tag set in BdistWheelCommand.
    if use_py_limited_api():
        cmake_extra_args += [
            '-DPYCBC_PY_LIMITED_API:BOOL=ON',
            f'-DPYCBC_PY_LIMITED_API_HEX={py_limited_api_hex()}',
        ]
    else:
        cmake_extra_args += ['-DPYCBC_PY_LIMITED_API:BOOL=OFF']

    # now pop these in CMAKE_COMMON_VARIABLES, and they will be used by cmake...
    os.environ['CMAKE_COMMON_VARIABLES'] = ' '.join(cmake_extra_args)


@dataclass
class CMakeConfig:
    build_type: str
    num_threads: int
    set_cpm_cache: bool
    env: Dict[str, str] = field(default_factory=dict)
    config_args: List[str] = field(default_factory=list)

    @classmethod
    def create_cmake_config(cls,  # noqa: C901
                            output_dir: str,
                            source_dir: str,
                            set_cpm_cache: Optional[bool] = None
                            ) -> CMakeConfig:
        env = os.environ.copy()
        num_threads = env.pop('PYCBC_CMAKE_PARALLEL_THREADS', '4')
        build_type = env.pop('PYCBC_BUILD_TYPE')
        cmake_generator = env.pop('PYCBC_CMAKE_SET_GENERATOR', None)
        cmake_arch = env.pop('PYCBC_CMAKE_SET_ARCH', None)
        # PYCBC_MODULE_OUTPUT_DIRECTORY is consumed by a per-target property, so only the
        # extension module lands in the package directory (PYCBC-1878).
        cmake_config_args = [CMAKE_EXE,
                             source_dir,
                             f'-DCMAKE_BUILD_TYPE={build_type}',
                             f'-DPYCBC_MODULE_OUTPUT_DIRECTORY={output_dir}']

        cmake_config_args.extend(
            [x for x in
                os.environ.get('CMAKE_COMMON_VARIABLES', '').split(' ')
                if x])

        python3_find_strategy = env.get('PYCBC_PYTHON3_FIND_STRATEGY', 'LOCATION')
        cmake_config_args += [f'-DPython3_FIND_STRATEGY={python3_find_strategy}']

        python3_rootdir = env.get('PYCBC_PYTHON3_ROOT_DIR', None)
        if python3_rootdir:
            cmake_config_args += [f'-DPython3_ROOT_DIR={python3_rootdir}']

        python3_executable = env.get('PYCBC_PYTHON3_EXECUTABLE', None)
        if python3_executable is None and sys.executable:
            # if sys.executable determines the path we want to use that to determine the
            # Python version in our get_python_version CMake function.
            python3_executable = sys.executable
            env['PYCBC_PYTHON3_EXECUTABLE'] = python3_executable
        if python3_executable:
            cmake_config_args += [f'-DPython3_EXECUTABLE={python3_executable}']

        python3_include = env.get('PYCBC_PYTHON3_INCLUDE_DIR', None)
        if python3_include:
            cmake_config_args += [f'-DPython3_INCLUDE_DIR={python3_include}']

        if set_cpm_cache is None:
            set_cpm_cache = env.pop('PYCBC_SET_CPM_CACHE', 'false').lower() in ENV_TRUE
        use_cpm_cache = env.pop('PYCBC_USE_CPM_CACHE', 'true').lower() in ENV_TRUE

        if set_cpm_cache is True:
            # if we are setting the cache, we don't want to attempt a build (it will fail).
            use_cpm_cache = False
            if os.path.exists(CXXCBC_CACHE_DIR):
                shutil.rmtree(CXXCBC_CACHE_DIR)
            cmake_config_args += [f'-DCOUCHBASE_CXX_CPM_CACHE_DIR={CXXCBC_CACHE_DIR}',
                                  '-DCPM_DOWNLOAD_ALL=ON',
                                  '-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON',
                                  '-DCPM_USE_LOCAL_PACKAGES=OFF']

        if use_cpm_cache is True:
            if not os.path.exists(CXXCBC_CACHE_DIR):
                raise OptionError(f'Cannot use cached dependencies, path={CXXCBC_CACHE_DIR} does not exist.')
            cmake_config_args += ['-DCPM_DOWNLOAD_ALL=OFF',
                                  '-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON',
                                  '-DCPM_USE_LOCAL_PACKAGES=OFF',
                                  f'-DCPM_SOURCE_CACHE={CXXCBC_CACHE_DIR}']
            # v4.3.4: If the user has not specifically provided what they want for downloading the Mozilla CA bundle,
            #         we turn this off to use the bundle from the CPM Cache.  If the user wants the bundle downloaded,
            #         we make sure to not set the CA_BUNDLE_ROOT path.
            user_defined_download_mozilla = next(
                (arg for arg in cmake_config_args if '-DDOWNLOAD_MOZILLA_CA_BUNDLE' in arg), None)
            if user_defined_download_mozilla is None:
                cmake_config_args += [f'-DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT={CXXCBC_CACHE_DIR}',
                                      '-DDOWNLOAD_MOZILLA_CA_BUNDLE:BOOL=OFF']
            elif user_defined_download_mozilla == '-DDOWNLOAD_MOZILLA_CA_BUNDLE:BOOL=OFF':
                cmake_config_args.append(f'-DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT={CXXCBC_CACHE_DIR}')

        if platform.system() == "Windows":
            if cmake_generator:
                if cmake_generator.upper() == 'TRUE':
                    cmake_config_args += ['-G', 'Visual Studio 16 2019']
                else:
                    cmake_config_args += ['-G', f'{cmake_generator}']

            if cmake_arch:
                if cmake_arch.upper() == 'TRUE':
                    if sys.maxsize > 2 ** 32:
                        cmake_config_args += ['-A', 'x64']
                else:
                    cmake_config_args += ['-A', f'{cmake_arch}']
            # maybe??
            # '-DCMAKE_WINDOWS_EXPORT_ALL_SYMBOLS=TRUE',

        return CMakeConfig(build_type,
                           num_threads,
                           set_cpm_cache,
                           env,
                           cmake_config_args)


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir='', py_limited_api=False):
        # py_limited_api is a documented setuptools Extension kwarg; passing it
        # via Extension.__init__ lets bdist_wheel auto-tag the wheel as abi3.
        Extension.__init__(self, name, sources=[], py_limited_api=py_limited_api)
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeConfigureExt(Command):
    description = 'Configure Python Operational SDK C Extension'
    user_options = []

    def initialize_options(self) -> None:
        return

    def finalize_options(self) -> None:
        return

    def run(self) -> None:
        check_for_cmake()
        process_build_env_vars()
        build_ext = self.get_finalized_command('build_ext')
        if len(self.distribution.ext_modules) != 1:
            raise SetupError('Should have only the Python SDK extension module.')
        ext = self.distribution.ext_modules[0]
        output_dir = os.path.abspath(os.path.dirname(build_ext.get_ext_fullpath(ext.name)))
        set_cpm_cache = os.environ.get('PYCBC_SET_CPM_CACHE', 'true').lower() in ENV_TRUE
        cmake_config = CMakeConfig.create_cmake_config(output_dir, ext.sourcedir, set_cpm_cache=set_cpm_cache)
        if not os.path.exists(build_ext.build_temp):
            os.makedirs(build_ext.build_temp)
        print(f'cmake config args: {cmake_config.config_args}')
        # configure (i.e. cmake ..)
        subprocess.check_call(cmake_config.config_args,  # nosec
                              cwd=build_ext.build_temp,
                              env=cmake_config.env)

        self._clean_cache_cpm_dependencies()

    def _clean_cache_cpm_dependencies(self):
        import re
        from fileinput import FileInput
        from pathlib import Path

        cxx_cache_path = Path(CXXCBC_CACHE_DIR)
        cmake_cpm = next((p for p in cxx_cache_path.glob('cpm/*') if f'{p}'.endswith('.cmake')), None)
        if cmake_cpm is not None:
            with FileInput(files=[cmake_cpm], inplace=True) as cpm_cmake:
                for line in cpm_cmake:
                    # used so that we don't have a dependency on git w/in environment
                    if 'find_package(Git REQUIRED)' in line:
                        line = re.sub(r'Git REQUIRED', 'Git', line)
                    # remove ending whitespace to avoid double spaced output
                    print(line.rstrip())


class CMakeBuildExt(build_ext):

    def get_ext_filename(self, ext_name):
        ext_path = ext_name.split('.')
        ext = next(
            (e for e in self.distribution.ext_modules if e.name == ext_name),
            None,
        )
        if ext is not None and getattr(ext, 'py_limited_api', False):
            # Stable-ABI build: emit `<name>.abi3.so` so the file CMake produces
            # (see CMakeLists.txt) matches what setuptools expects to find on disk,
            # regardless of which interpreter compiled it. Windows is the exception:
            # importlib knows only `.cp3XY-win_amd64.pyd` and `.pyd` there, so an
            # `.abi3.pyd` is unimportable and the module must stay `<name>.pyd`.
            so_ext = "." + get_config_var('EXT_SUFFIX').split('.')[-1]
            ext_suffix = so_ext if platform.system() == 'Windows' else '.abi3' + so_ext
        else:
            # Full C-API build: keep the interpreter-specific suffix (e.g.
            # .cpython-310-darwin.so) so this extension can't be picked up
            # by a mismatched interpreter's import machinery. A bare .so
            # would be loadable by any version, defeating the point of a
            # non-limited build.
            ext_suffix = get_config_var('EXT_SUFFIX')
        return os.path.join(*ext_path) + ext_suffix

    def build_extension(self, ext):  # noqa: C901
        check_for_cmake()
        process_build_env_vars()
        if isinstance(ext, CMakeExtension):
            output_dir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
            cmake_config = CMakeConfig.create_cmake_config(output_dir, ext.sourcedir)

            cmake_build_args = [CMAKE_EXE,
                                '--build',
                                '.',
                                '--config',
                                f'{cmake_config.build_type}',
                                '--parallel',
                                f'{cmake_config.num_threads}']

            if not os.path.exists(self.build_temp):
                os.makedirs(self.build_temp)
            print(f'cmake config args: {cmake_config.config_args}')
            # configure (i.e. cmake ..)
            subprocess.check_call(cmake_config.config_args,  # nosec
                                  cwd=self.build_temp,
                                  env=cmake_config.env)
            print(f'cmake build args: {cmake_build_args}')
            # build (i.e. cmake --build .)
            subprocess.check_call(cmake_build_args,  # nosec
                                  cwd=self.build_temp,
                                  env=cmake_config.env)

        else:
            super().build_extension(ext)

    def _clean_cache_cpm_dependencies(self):
        import re
        from fileinput import FileInput
        from pathlib import Path

        cxx_cache_path = Path(CXXCBC_CACHE_DIR)
        cmake_cpm = next((p for p in cxx_cache_path.glob('cpm/*') if f'{p}'.endswith('.cmake')), None)
        if cmake_cpm is not None:
            with FileInput(files=[cmake_cpm], inplace=True) as cpm_cmake:
                for line in cpm_cmake:
                    # used so that we don't have a dependency on git w/in environment
                    if 'find_package(Git REQUIRED)' in line:
                        line = re.sub(r'Git REQUIRED', 'Git', line)
                    # remove ending whitespace to avoid double spaced output
                    print(line.rstrip())


# bdist_wheel moved from the `wheel` package into setuptools in setuptools 70.1.
# Prefer the setuptools-vendored location so we work without an explicit `wheel`
# build dependency, but fall back for older setuptools.
try:
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover - older setuptools
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel  # type: ignore[no-redef]


class BdistWheelCommand(_bdist_wheel):
    """bdist_wheel that tags the wheel as abi3 when building against Py_LIMITED_API.

    `pip wheel .` invokes bdist_wheel without flags, so even if Extension
    declares py_limited_api=True the wheel still gets a CPython-specific tag.
    Default --py-limited-api here so the produced wheel is named
    `<dist>-<ver>-cp310-abi3-<platform>.whl` and installs onto any CPython >=3.10.

    Both the wheel tag and the C-level Py_LIMITED_API hex come from
    py_limited_api_version() so they cannot drift apart.
    """

    def finalize_options(self):
        if use_py_limited_api() and not self.py_limited_api:
            self.py_limited_api = py_limited_api_wheel_tag()
        super().finalize_options()


class BuildCommand(build):
    def finalize_options(self):
        # Setting the build_base to an absolute path will make sure that build (i.e. temp) and lib dirs are in sync
        # and that our binary is copied appropriately after the build is complete. Particularly useful to avoid Windows
        # complaining about long paths.
        # NOTE:  if setting the build_temp and/or build_lib, the paths should include the build_base path.
        #   EX: PYCBC_BUILD_BASE=C:\Users\Admin\build
        #       PYCBC_BUILD_TEMP=C:\Users\Admin\build\tmp
        #       PYCBC_BUILD_LIB=C:\Users\Admin\build\lib
        env = os.environ.copy()
        pycbc_build_base = env.pop('PYCBC_BUILD_BASE', None)
        if pycbc_build_base:
            self.build_base = pycbc_build_base
        pycbc_build_temp = env.pop('PYCBC_BUILD_TEMP', None)
        if pycbc_build_temp:
            self.build_temp = pycbc_build_temp
        pycbc_build_lib = env.pop('PYCBC_BUILD_LIB', None)
        if pycbc_build_lib:
            self.build_lib = pycbc_build_lib
        super().finalize_options()
