#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os.path
import platform
import re
import subprocess
import sys

from distutils.version import LooseVersion

from setuptools import Extension
from setuptools.command.build_ext import build_ext

import cbuild_config
from gen_config import win_cmake_path
from cbuild_config import couchbase_core, build_type
import logging
import traceback


PYCBC_SSL_FETCH = os.getenv('PYCBC_SSL_FETCH', '')


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir='', **kw):
        Extension.__init__(self, name, **kw)
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(cbuild_config.CBuildCommon):
    hasrun = False
    hasbuilt = False
    hybrid = False
    info = None  # type: cbuild_config.CBuildInfo

    def run(self):
        if not CMakeBuild.hasrun:
            out = self.check_for_cmake()
            if not out:
                raise RuntimeError(
                    "CMake must be installed to build the following extensions: " +
                    ", ".join(e.name for e in self.extensions))

            if platform.system() == "Windows":
                cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)',
                                                       out.decode()).group(1))
                if cmake_version < '3.1.0':
                    raise RuntimeError("CMake >= 3.1.0 is required on Windows")
            CMakeBuild.hasrun = True
            if CMakeBuild.hybrid:
                build_ext.run(self)

            for ext in self.extensions:
                self.build_extension(ext)

    @staticmethod
    def check_for_cmake():
        try:
            return subprocess.check_output(['cmake', '--version'])
        except:
            return None

    @staticmethod
    def requires():
        base_req = []
        if re.match(r'.*(CONAN|ALL).*', PYCBC_SSL_FETCH):
            base_req.append('conan')
        if re.match(r'.*(GITHUB_API|ALL).*', PYCBC_SSL_FETCH):
            base_req.append('PyGithub')
        return base_req + ([] if CMakeBuild.check_for_cmake() else ["cmake"])

    def prep_build(self, ext):
        if not CMakeBuild.hasbuilt:
            from distutils.sysconfig import get_python_inc
            import distutils.sysconfig as sysconfig
            cfg = self.cfg_type()
            extdir = os.path.abspath(
                os.path.dirname(self.get_ext_fullpath(ext.name)))

            lcb_api_flags = self.get_lcb_api_flags()
            cmake_args = lcb_api_flags + ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                                          '-DPYTHON_EXECUTABLE=' + sys.executable]
            cmake_args += ['-DPYTHON_INCLUDE_DIR={}'.format(get_python_inc())]
            self.info.setbase(self.build_temp)
            self.info.cfg=cfg
            from distutils import sysconfig
            import os.path as op
            v = sysconfig.get_config_vars()
            print("LIBDIR {}, LIBPL {}".format(v.get("LIBDIR"), v.get("LIBPL")))
            fpaths = [op.join(v.get(pv, ''), v.get('LDLIBRARY', '')) for pv in ('LIBDIR', 'LIBPL')] + [os.path.normpath(
                os.path.join(get_python_inc(), "..", "..", "lib",
                             "libpython{}.dylib".format('.'.join(map(str, sys.version_info[0:2]))))),
                os.path.join(get_python_inc(), "..", "..", "lib")]
            python_lib = None
            python_libdir = None
            for entry in fpaths:
                if not op.exists(entry):
                    print("fpath {} does not exist".format(entry))
                    continue
                try:
                    print("got fpath {}:".format(entry))
                    if op.isfile(entry):
                        print("fpath {} is file, selecting".format(entry))
                        python_lib = python_lib or entry
                        continue
                    else:
                        entries = os.listdir(entry)
                        print("fpath {} is directory, contents {}".format(entry, entries))
                        for subentry in entries:
                            fullname = op.normpath(op.join(entry, subentry))
                            try:
                                fullname = op.readlink(fullname)
                            except:
                                pass
                            print("trying subentry:{}".format(fullname))

                            if op.exists(fullname):
                                python_lib = python_lib or fullname
                                python_libdir = op.normpath(entry)
                                print("got match {}, breaking out".format(fullname))
                                continue

                except:
                    pass
            cmake_args += ['-DHYBRID_BUILD=TRUE'] if CMakeBuild.hybrid else []
            cmake_args += ['-DPYTHON_LIBFILE={}'.format(python_lib)] if python_lib else []
            cmake_args += ['-DPYTHON_LIBDIR={}'.format(python_libdir)] if python_libdir else []
            cmake_args += [
                '-DPYTHON_VERSION_EXACT={}'.format('.'.join(map(str, sys.version_info[0:2])))] if python_libdir else []
            build_args = ['--config', cfg]
            if platform.system() == "Windows":
                cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(
                    cfg.upper(),
                    extdir), '-DLCB_NO_MOCK=1',
                    '-DCMAKE_BUILD_PARALLEL_LEVEL=1']
                if sys.maxsize > 2 ** 32:
                    cmake_args += ['-A', 'x64']
                build_args += ['--', '/m']
            else:
                cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg.upper()]
                build_args += ['--', '-j2']
            env = os.environ.copy()
            python_executable = win_cmake_path(sys.executable)
            pass_path = False
            if re.match(r'.*(CONAN|ALL).*',PYCBC_SSL_FETCH):
                try:
                    import conans.conan
                    env['PATH'] = env['PATH']+";{}".format(os.path.dirname(conans.conan.__file__))
                    pass_path = True
                except:
                    logging.warning("Cannot find conan : {}".format(traceback.format_exc()))
            if re.match(r'.*(GITHUB|ALL).*', PYCBC_SSL_FETCH):
                pass_path = True
            if pass_path:
                pathsep = ';' if platform.system().lower().startswith('win') else ':'
                env['PYTHONPATH'] = pathsep.join(sys.path)
            cmake_args += [
                           '-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON',
                           '-DPYTHON_EXECUTABLE={}'.format(python_executable)]
            if PYCBC_SSL_FETCH:
                cmake_args += ['-DPYCBC_SSL_FETCH={}'.format(PYCBC_SSL_FETCH)]
            PYCBC_CMAKE_DEBUG = env.get('PYCBC_CMAKE_DEBUG')
            if PYCBC_CMAKE_DEBUG:
                cmake_args += [
                '--trace-source=CMakeLists.txt',
                '--trace-expand']
            cxx_compile_args=filter(re.compile(r'^(?!-std\s*=\s*c(11|99)).*').match, ext.extra_compile_args)
            env['CXXFLAGS'] = '{} {} -DVERSION_INFO=\\"{}\\"'.format(
                env.get('CXXFLAGS', ''), ' '.join(cxx_compile_args),
                self.distribution.get_version())

            env['CFLAGS'] = '{} {}'.format(
                env.get('CFLAGS', ''), ' '.join(ext.extra_compile_args),
                self.distribution.get_version())
            print("Launching build with env: {}, build_args: {}, cmake_args: {}".format(env, build_args, cmake_args))
            if not os.path.exists(self.build_temp):
                os.makedirs(self.build_temp)
            subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, stdout=sys.stdout, stderr=sys.stdout,
                                  cwd=self.build_temp, env=env)
            subprocess.check_call(['cmake', '--build', '.'] + build_args,
                                  cwd=self.build_temp)
            CMakeBuild.hasbuilt = True
            build_dir = os.path.realpath(self.build_lib)

            if CMakeBuild.hybrid:
                if not self.compiler:
                    self.run()

            for name in self.info.entries():
                try:
                    pkg_build_dir=os.path.join(build_dir, cbuild_config.couchbase_core)
                    self.copy_binary_to(cfg, pkg_build_dir, self.info.lcb_pkgs_srcs(), name)
                    self.copy_binary_to(cfg, self.info.pkg_data_dir, self.info.lcb_pkgs_srcs(), name)
                except:
                    print("failure")
                    raise


def gen_cmake_build(extoptions, pkgdata):
    CMakeBuild.hybrid = build_type in ['CMAKE_HYBRID']
    CMakeBuild.setup_build_info(extoptions, pkgdata)
    e_mods = [CMakeExtension(str(couchbase_core+'._libcouchbase'), '', **extoptions)]
    return e_mods, CMakeBuild.requires(), cbuild_config.LazyCommandClass(CMakeBuild)


