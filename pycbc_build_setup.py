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

import os
import platform
import shutil
import subprocess  # nosec
import sys
from sysconfig import get_config_var

from setuptools import Extension
from setuptools.command.build import build
from setuptools.command.build_ext import build_ext

CMAKE_EXE = os.environ.get('CMAKE_EXE', shutil.which('cmake'))
PYCBC_ROOT = os.path.dirname(__file__)
# PYCBC_CXXCBC_CACHE_DIR should only need to be used on Windows when setting the CPM cache (PYCBC_SET_CPM_CACHE=ON).
# It helps prevent issues w/ path lengths.
# NOTE: Setting the CPM cache on a Windows machine should be a _rare_ occasion.  When doing so and setting
# PYCBC_CXXCBC_CACHE_DIR, be sure to copy the cache to <root source dir>\deps\couchbase-cxx-cache if building a sdist.
CXXCBC_CACHE_DIR = os.environ.get('PYCBC_CXXCBC_CACHE_DIR', os.path.join(PYCBC_ROOT, 'deps', 'couchbase-cxx-cache'))
ENV_TRUE = ['true', '1', 'y', 'yes', 'on']


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
    if pycbc_use_openssl is True:
        cmake_extra_args += ['-DUSE_STATIC_BORINGSSL:BOOL=OFF']
        ssl_version = os.getenv('PYCBC_OPENSSL_VERSION', None)
        if not ssl_version:
            ssl_version = '1.1.1w'
        cmake_extra_args += [f'-DOPENSSL_VERSION={ssl_version}']
    else:
        cmake_extra_args += ['-DUSE_STATIC_BORINGSSL:BOOL=ON']

    # v4.1.9: building with static stdlibc++ must be opted-in by user
    use_static_stdlib = os.getenv('PYCBC_USE_STATIC_STDLIB', 'false').lower() in ENV_TRUE
    if use_static_stdlib is True:
        cmake_extra_args += ['-DUSE_STATIC_STDLIB:BOOL=ON']
    else:
        cmake_extra_args += ['-DUSE_STATIC_STDLIB:BOOL=OFF']

    sanitizers = os.getenv('PYCBC_SANITIZERS', None)
    if sanitizers:
        for x in sanitizers.split(','):
            cmake_extra_args += [f'-DENABLE_SANITIZER_{x.upper()}=ON']

    if os.getenv('PYCBC_VERBOSE_MAKEFILE', None):
        cmake_extra_args += ['-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON']

    pycbc_cmake_system_version = os.getenv('PYCBC_CMAKE_SYSTEM_VERSION', None)
    if pycbc_cmake_system_version is not None:
        cmake_extra_args += [f'-DCMAKE_SYSTEM_VERSION={pycbc_cmake_system_version}']

    # now pop these in CMAKE_COMMON_VARIABLES, and they will be used by cmake...
    os.environ['CMAKE_COMMON_VARIABLES'] = ' '.join(cmake_extra_args)


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        check_for_cmake()
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuildExt(build_ext):

    def get_ext_filename(self, ext_name):
        ext_path = ext_name.split('.')
        ext_suffix = get_config_var('EXT_SUFFIX')
        ext_suffix = "." + ext_suffix.split('.')[-1]
        return os.path.join(*ext_path) + ext_suffix

    def build_extension(self, ext):  # noqa: C901
        check_for_cmake()
        process_build_env_vars()
        if isinstance(ext, CMakeExtension):
            env = os.environ.copy()
            output_dir = os.path.abspath(
                os.path.dirname(self.get_ext_fullpath(ext.name)))

            num_threads = env.pop('PYCBC_CMAKE_PARALLEL_THREADS', '4')
            build_type = env.pop('PYCBC_BUILD_TYPE')
            cmake_generator = env.pop('PYCBC_CMAKE_SET_GENERATOR', None)
            cmake_arch = env.pop('PYCBC_CMAKE_SET_ARCH', None)

            cmake_config_args = [CMAKE_EXE,
                                 ext.sourcedir,
                                 f'-DCMAKE_BUILD_TYPE={build_type}',
                                 f'-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={output_dir}',
                                 f'-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{build_type.upper()}={output_dir}']

            cmake_config_args.extend(
                [x for x in
                    os.environ.get('CMAKE_COMMON_VARIABLES', '').split(' ')
                    if x])

            python3_executable = env.pop('PYCBC_PYTHON3_EXECUTABLE', None)
            if python3_executable:
                cmake_config_args += [f'-DPython3_EXECUTABLE={python3_executable}']

            python3_include = env.pop('PYCBC_PYTHON3_INCLUDE_DIR', None)
            if python3_include:
                cmake_config_args += [f'-DPython3_INCLUDE_DIR={python3_include}']

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
                    raise Exception(f'Cannot use cached dependencies, path={CXXCBC_CACHE_DIR} does not exist.')
                cmake_config_args += ['-DCPM_DOWNLOAD_ALL=OFF',
                                      '-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON',
                                      '-DCPM_USE_LOCAL_PACKAGES=OFF',
                                      f'-DCPM_SOURCE_CACHE={CXXCBC_CACHE_DIR}',
                                      f'-DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT={CXXCBC_CACHE_DIR}"']

            if platform.system() == "Windows":
                cmake_config_args += [f'-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_{build_type.upper()}={output_dir}']

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

            cmake_build_args = [CMAKE_EXE,
                                '--build',
                                '.',
                                '--config',
                                f'{build_type}',
                                '--parallel',
                                f'{num_threads}']

            if not os.path.exists(self.build_temp):
                os.makedirs(self.build_temp)
            print(f'cmake config args: {cmake_config_args}')
            # configure (i.e. cmake ..)
            subprocess.check_call(cmake_config_args,  # nosec
                                  cwd=self.build_temp,
                                  env=env)

            if set_cpm_cache is True:
                self._clean_cache_cpm_dependencies()
                # NOTE: since we are not building, this will create an error.  Okay, as attempting the
                # build will fail anyway and we really just want to update the CXX cache.
            else:
                print(f'cmake build args: {cmake_build_args}')
                # build (i.e. cmake --build .)
                subprocess.check_call(cmake_build_args,  # nosec
                                      cwd=self.build_temp,
                                      env=env)

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
