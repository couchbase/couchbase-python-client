#  Copyright 2016-2022. Couchbase, Inc.
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

from setuptools import (Extension,
                        find_packages,
                        setup)
from setuptools.command.build_ext import build_ext

sys.path.append('.')
import couchbase_version  # nopep8 # isort:skip # noqa: E402

CMAKE_EXE = os.environ.get('CMAKE_EXE', shutil.which('cmake'))

try:
    couchbase_version.gen_version()
except couchbase_version.CantInvokeGit:
    pass

COUCHBASE_VERSION = couchbase_version.get_version()

COUCHBASE_README = os.path.join(os.path.dirname(__file__), 'README.md')


def check_for_cmake():
    if not CMAKE_EXE:
        print('cmake executable not found. '
              'Set CMAKE_EXE environment or update your path')
        sys.exit(1)


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
        if isinstance(ext, CMakeExtension):

            # Don't necessarily like this, but when calling setup.py bdist_wheel, the build_ext command
            # will be invoked and setuptools does not provide a way to pass in the 'build-temp' and
            # 'build-lib' options like you can when invoking setup.py build_ext from the command line.
            # Setting these variables here is a work-around.
            #
            # Seems to be a problem in Windows environments specifically trying to avoid the issue
            # with long paths > 260 chars. Ugh -- Windows!! :/
            env = os.environ.copy()
            pycbc_build_temp = env.pop('PYCBC_BUILD_TEMP', None)
            if pycbc_build_temp:
                self.build_temp = pycbc_build_temp

            pycbc_build_lib = env.pop('PYCBC_BUILD_LIB', None)
            if pycbc_build_lib:
                self.build_lib = pycbc_build_lib

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
            print(f'cmake build args: {cmake_build_args}')
            # build (i.e. cmake --build .)
            subprocess.check_call(cmake_build_args,  # nosec
                                  cwd=self.build_temp,
                                  env=env)
        else:
            super().build_extension(ext)


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
# os.environ['CXXFLAGS'] = '{} {}'.format(os.getenv('CXXFLAGS', ''), '-mmacosx-version-min=10.10')
os.environ['PYCBC_BUILD_TYPE'] = build_type
cmake_extra_args = []

# TODO: lets figure out why the cmake finder doesn't find the homebrew-installed openssl (on Mac).
# for now we need to use this flag.
ssl_dir = os.getenv('PYCBC_OPENSSL_DIR')
if ssl_dir:
    cmake_extra_args += [f'-DOPENSSL_ROOT_DIR={ssl_dir}']

ssl_version = os.getenv('PYCBC_OPENSSL_VERSION', '1.1.1g')
cmake_extra_args += [f'-DOPENSSL_VERSION={ssl_version}']

sanitizers = os.getenv('PYCBC_SANITIZERS')
if sanitizers:
    for x in sanitizers.split(','):
        cmake_extra_args += [f'-DENABLE_SANITIZER_{x.upper()}=ON']

if os.getenv('PYCBC_VERBOSE_MAKEFILE', None):
    cmake_extra_args += ['-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON']

# now pop these in CMAKE_COMMON_VARIABLES, and they will be used by cmake...
os.environ['CMAKE_COMMON_VARIABLES'] = ' '.join(cmake_extra_args)

package_data = {}
# some more Windows tomfoolery...
if platform.system() == 'Windows':
    package_data = {'couchbase': ['pycbc_core.pyd']}

print(f'Python SDK version: {COUCHBASE_VERSION}')

setup(name='couchbase',
      version=COUCHBASE_VERSION,
      ext_modules=[CMakeExtension('couchbase.pycbc_core')],
      cmdclass={'build_ext': CMakeBuildExt},
      python_requires='>=3.7',
      packages=find_packages(
          include=['acouchbase', 'couchbase', 'txcouchbase', 'couchbase.*', 'acouchbase.*', 'txcouchbase.*'],
          exclude=['acouchbase.tests', 'couchbase.tests', 'txcouchbase.tests']),
      package_data=package_data,
      url="https://github.com/couchbase/couchbase-python-client",
      author="Couchbase, Inc.",
      author_email="PythonPackage@couchbase.com",
      license="Apache License 2.0",
      description="Python Client for Couchbase",
      long_description=open(COUCHBASE_README, "r").read(),
      long_description_content_type='text/markdown',
      keywords=["couchbase", "nosql", "pycouchbase", "libcouchbase"],
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: Implementation :: CPython",
          "Topic :: Database",
          "Topic :: Software Development :: Libraries",
          "Topic :: Software Development :: Libraries :: Python Modules"],
      )
