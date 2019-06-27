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
from cbuild_config import couchbase_core, build_type


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
        return "" if CMakeBuild.check_for_cmake() else ["cmake"]

    def prep_build(self, ext):
        if not CMakeBuild.hasbuilt:
            from distutils.sysconfig import get_python_inc
            import distutils.sysconfig as sysconfig
            cfg = self.cfg_type()
            extdir = os.path.abspath(
                os.path.dirname(self.get_ext_fullpath(ext.name)))
            pycbc_lcb_api=os.getenv("PYCBC_LCB_API",
                                    cbuild_config.BUILD_CFG.get('comp_options', {}).get('PYCBC_LCB_API', None))
            lcb_api_flags = ['-DPYCBC_LCB_API={}'.format(pycbc_lcb_api)] if pycbc_lcb_api else []
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
                    extdir), '-DLCB_NO_MOCK=1', '-DLCB_NO_SSL=1']
                if sys.maxsize > 2 ** 32:
                    cmake_args += ['-A', 'x64']
                build_args += ['--', '/m']
            else:
                cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
                build_args += ['--', '-j2']

            env = os.environ.copy()
            cmake_args += ['-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON']
            env['CXXFLAGS'] = '{} {} -DVERSION_INFO=\\"{}\\"'.format(
                env.get('CXXFLAGS', ''), ' '.join(ext.extra_compile_args),
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

            if CMakeBuild.hybrid:
                from distutils.ccompiler import CCompiler
                ext.extra_compile_args += lcb_api_flags
                compiler = self.compiler  # type: CCompiler

                lcb_include = os.path.join(self.build_temp, "install", "include")
                compiler.add_include_dir(lcb_include)
                lib_dirs = [self.info.pkg_data_dir]+self.info.get_lcb_dirs()
                try:
                    existing_lib_dirs=compiler.library_dirs
                    compiler.set_library_dirs(lib_dirs+existing_lib_dirs)
                except:
                    compiler.add_library_dirs(lib_dirs)


def gen_cmake_build(extoptions, pkgdata):
    from cmake_build import CMakeExtension, CMakeBuild

    class LazyCommandClass(dict):
        """
        Lazy command class that defers operations requiring given cmdclass until
        they've actually been downloaded and installed by setup_requires.
        """
        def __init__(self, cmdclass_real):
            self.cmdclass_real=cmdclass_real

        def __contains__(self, key):
            return (
                    key == 'build_ext'
                    or super(LazyCommandClass, self).__contains__(key)
            )

        def __setitem__(self, key, value):
            if key == 'build_ext':
                raise AssertionError("build_ext overridden!")
            super(LazyCommandClass, self).__setitem__(key, value)

        def __getitem__(self, key):
            if key != 'build_ext':
                return super(LazyCommandClass, self).__getitem__(key)
            return self.cmdclass_real

    CMakeBuild.hybrid = build_type in ['CMAKE_HYBRID']
    CMakeBuild.setup_build_info(extoptions, pkgdata)
    e_mods = [CMakeExtension(str(couchbase_core+'._libcouchbase'), '', **extoptions)]
    return e_mods, CMakeBuild.requires(), LazyCommandClass(CMakeBuild)


