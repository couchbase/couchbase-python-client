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
from shutil import copyfile, copymode

from setuptools import Extension
from setuptools.command.build_ext import build_ext

import cbuild_config


def get_plat_code():
    plat = sys.platform.lower()
    substitutions = {'win': r'^win.*$'}
    for target, pattern in substitutions.items():
        plat = re.compile(pattern).sub(target, plat)
    return plat


class CMakeBuildInfo:
    def __init__(self, cmake_base=None):
        self.setbase(cmake_base)
        self.cfg="Release"
        self.pkg_data_dir=os.path.join(cbuild_config.couchbase_core)
    @property
    def base(self):
        print("self.base is {}".format(self._cmake_base))

        return self._cmake_base

    def setbase(self, path):
        self._cmake_base=(path if isinstance(path,list) else list(os.path.split(path))) if path else None
        print("set base as {}".format(self._cmake_base))

    @base.setter
    def base(self, path):
        self.setbase(path)

    def entries(self):
        plat = get_plat_code()

        print("Got platform {}".format(plat))
        default = ['libcouchbase.so', 'libcouchbase.so.2', 'libcouchbase.so.3']
        return {'darwin': ['libcouchbase.2.dylib', 'libcouchbase.dylib'], 'linux': default,
                'win': ['libcouchbase_d.dll','libcouchbase.dll']}.get(get_plat_code(), default)

    def lcb_build_base(self):
        print("self.base is {}".format(self.base))
        return self._cmake_base + ['install', 'lib']

    def lcb_pkgs_srcs(self):
        return {'Debug':self.lcb_build_base() + ['Debug'],'Release':self.lcb_build_base() + ['Release']}

    def lcb_pkgs(self, cfg):
        return map(lambda x: self.lcb_pkgs_srcs()[cfg] + [x], self.entries())

    def lcb_pkgs_strlist(self):
        print("got pkgs {}".format(self.entries()))
        for x in self.entries():
            print("yielding binary {} : {}".format(x, os.path.join(self.pkg_data_dir,x)))
            yield os.path.join(self.pkg_data_dir, x)


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir='', **kw):
        Extension.__init__(self, name, **kw)
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    hasrun = False
    hasbuilt = False
    hybrid = False

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

    def build_extension(self, ext):
        if not CMakeBuild.hasbuilt:
            from distutils.sysconfig import get_python_inc
            import distutils.sysconfig as sysconfig
            cfg = 'Debug' if self.debug else 'Release'
            extdir = os.path.abspath(
                os.path.dirname(self.get_ext_fullpath(ext.name)))
            import cmodule
            pycbc_lcb_api=os.getenv("PYCBC_LCB_API",
                                    cbuild_config.BUILD_CFG.get('comp_options', {}).get('PYCBC_LCB_API', None))
            lcb_api_flags = ['-DPYCBC_LCB_API={}'.format(pycbc_lcb_api)] if pycbc_lcb_api else []
            cmake_args = lcb_api_flags + ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                                          '-DPYTHON_EXECUTABLE=' + sys.executable]
            cmake_args += ['-DPYTHON_INCLUDE_DIR={}'.format(get_python_inc())]
            self.info.setbase(self.build_temp)
            self.info.cfg=cfg
            lib_paths=self.info.lcb_pkgs_srcs()
            lib_path = os.path.join(*lib_paths[cfg])
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
            rpaths = [{'Darwin': '@loader_path', 'Linux': '$ORIGIN'}.get(platform.system(), "$ORIGIN"),
                      lib_path]
            print("adding rpaths {}".format(rpaths))
            build_dir = os.path.realpath(self.build_lib)

            if CMakeBuild.hybrid:
                if not self.compiler:
                    self.run()

            for name in self.info.entries():
                try:
                    pkg_build_dir=os.path.join(build_dir, cbuild_config.couchbase_core)
                    self.copy_binary_to(cfg, pkg_build_dir, lib_paths, name)
                    self.copy_binary_to(cfg, self.info.pkg_data_dir, lib_paths, name)
                except:
                    print("failure")
                    raise

            if CMakeBuild.hybrid:
                from distutils.ccompiler import CCompiler
                ext.extra_compile_args += lcb_api_flags
                compiler = self.compiler  # type: CCompiler

                lcb_include = os.path.join(self.build_temp, "install", "include")
                compiler.add_include_dir(lcb_include)
                lib_dirs = [self.info.pkg_data_dir]+self.get_lcb_dirs()
                try:
                    existing_lib_dirs=compiler.library_dirs
                    compiler.set_library_dirs(lib_dirs+existing_lib_dirs)
                except:
                    compiler.add_library_dirs(lib_dirs)
                if platform.system() != 'Windows':
                    try:
                        existing_rpaths=compiler.runtime_library_dirs
                        compiler.set_runtime_library_dirs(rpaths+existing_rpaths)
                    except:
                        pass
                    for rpath in rpaths:
                        compiler.add_runtime_library_dir(rpath)
                        ext.extra_link_args.insert(0,'-Wl,-rpath,' + rpath)
                build_ext.build_extension(self, ext)

    def get_lcb_dirs(self):
        lcb_dbg_build = os.path.join(*(self.info.base + ["install", "lib", "Debug"]))
        lcb_build = os.path.join(*(self.info.base + ["install", "lib", "Release"]))
        lib_dirs = [lcb_dbg_build, lcb_build]
        return lib_dirs

    def copy_binary_to(self, cfg, dest_dir, lib_paths, name):
        try:
            os.makedirs(dest_dir)
        except:
            pass
        dest = os.path.join(dest_dir, name)
        failures = {}
        lib_paths_prioritized = [(k, v) for k, v in lib_paths.items() if k == cfg]
        lib_paths_prioritized += [(k, v) for k, v in lib_paths.items() if k != cfg]
        for rel_type, binary_path in lib_paths_prioritized:
            src = os.path.join(*(binary_path + [name]))
            try:
                if os.path.exists(src):
                    print("copying {} to {}".format(src, dest))
                    copyfile(src, dest)
                    print("success")
            except Exception as e:
                failures[rel_type] = "copying {} to {}, got {}".format(src, dest, repr(e))
        if len(failures) == len(lib_paths):
            raise Exception("Failed to copy binary: {}".format(failures))

    def copy_test_file(self, src_file):
        '''
        Copy ``src_file`` to ``dest_file`` ensuring parent directory exists.
        By default, message like `creating directory /path/to/package` and
        `copying directory /src/path/to/package -> path/to/package` are displayed on standard output. Adapted from scikit-build.
        '''
        # Create directory if needed
        dest_dir = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'tests', 'bin')
        if dest_dir != "" and not os.path.exists(dest_dir):
            print("creating directory {}".format(dest_dir))
            os.makedirs(dest_dir)

        # Copy file
        dest_file = os.path.join(dest_dir, os.path.basename(src_file))
        print("copying {} -> {}".format(src_file, dest_file))
        copyfile(src_file, dest_file)
        copymode(src_file, dest_file)
