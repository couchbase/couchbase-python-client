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

import itertools
import json
import os
import os.path
import platform
import re
import sys
import warnings
from distutils.command.install_headers import install_headers as install_headers_orig
from shutil import copyfile, copymode

from setuptools.command.build_ext import build_ext
import pathlib
import gen_config


curdir = pathlib.Path(__file__).parent


def get_json_build_cfg():
    with open(str(curdir.joinpath("cbuild_cfg.json"))) as JSONFILE:
        return json.load(JSONFILE)


BUILD_CFG = get_json_build_cfg()
PYCBC_LCB_API = os.getenv("PYCBC_LCB_API", BUILD_CFG.get('comp_options', {}).get('PYCBC_LCB_API'))


def get_all_sources():
    return BUILD_CFG.get('source', []) + BUILD_CFG.get('apis', {}).get(PYCBC_LCB_API, {}).get('sources', [])


def get_sources():
    sources_ext={}
    all_sources = get_all_sources()
    SOURCEMODS = list(filter(re.compile(r'^.*\.c$').match, all_sources))
    SOURCEMODS_CPP = list(filter(re.compile(r'^.*\.(cpp|cxx|cc)$').match, all_sources))
    sources_ext['sources'] = list(map(str, SOURCEMODS+SOURCEMODS_CPP))
    return sources_ext


couchbase_core = BUILD_CFG.get("comp_options",{}).get("PYCBC_CORE","couchbase")


def get_cbuild_options():
    extoptions={}
    extoptions['extra_compile_args'] = []
    extoptions['extra_link_args'] = []

    def boolean_option(flag):
        return ["-D{}={}".format(flag, os.environ.get(flag))]

    def string_option(flag):
        return ["-D{}={}".format(flag, os.environ.get(flag))]

    COMP_OPTION_PREFIX = "PYCBC_COMP_OPT_"

    def comp_option(flag):
        return ["-{}={}".format(flag.replace(COMP_OPTION_PREFIX, ""), os.environ.get(flag))]

    COMP_OPTION_BOOL_PREFIX = "PYCBC_COMP_OPT_BOOL_"

    def comp_option_bool(flag):
        return ["-{}".format(flag.replace(COMP_OPTION_BOOL_PREFIX, ""))]

    CLANG_SAN_OPTIONS = {"address": "lsan", "undefined": "ubsan"}
    CLANG_SAN_PREFIX = "PYCBC_SAN_OPT_"

    def comp_clang_san_option(flag):
        san_option = flag.replace(CLANG_SAN_PREFIX, "")
        fsanitize_statements = ["-fsanitize={}".format(san_option), "-fno-omit-frame-pointer"]
        extoptions['extra_link_args'] += fsanitize_statements + ['-Llibclang_rt.asan_osx_dynamic']
        return fsanitize_statements

    def comp_option_pattern(prefix):
        return re.escape(prefix) + ".*"

    comp_flags = {"PYCBC_STRICT": boolean_option,
                  "PYCBC_TABBED_CONTEXTS_ENABLE": boolean_option,
                  "PYCBC_LCB_API": string_option,
                  "PYCBC_REF_ACCOUNTING": boolean_option,
                  "PYCBC_TRACING_DISABLE": boolean_option, "PYCBC_DEBUG": boolean_option,
                  "PYCBC_GEN_PYTHON": boolean_option,
                  "PYCBC_CRYPTO_VERSION": boolean_option, comp_option_pattern(COMP_OPTION_PREFIX): comp_option,
                  comp_option_pattern(COMP_OPTION_BOOL_PREFIX): comp_option_bool,
                  comp_option_pattern(CLANG_SAN_PREFIX): comp_clang_san_option}
    debug_symbols = len(set(os.environ.keys()) & {"PYCBC_DEBUG", "PYCBC_DEBUG_SYMBOLS"}) > 0
    comp_arg_additions = list(itertools.chain.from_iterable(
        action(actual_flag) for flag, action in comp_flags.items() for actual_flag in os.environ.keys() if
        re.match(flag, actual_flag)))
    print(comp_arg_additions)
    extoptions['include_dirs'] = []
    extoptions['extra_compile_args'] += list(comp_arg_additions)
    return extoptions, debug_symbols


def get_ext_options():
    extoptions, debug_symbols = get_cbuild_options()
    pkgdata = {}
    if sys.platform != 'win32':
        extoptions['extra_compile_args'] += ['-Wno-strict-prototypes', '-fPIC','-std=c11']
        extoptions['libraries'] = ['couchbase']
        if debug_symbols:
            extoptions['extra_compile_args'] += ['-O0', '-g3']
            extoptions['extra_link_args'] += ['-O0', '-g3']
        if sys.platform == 'darwin':
            extoptions['library_dirs'] = ['/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/10.0.0/lib/darwin/']
            extoptions['extra_compile_args']+=['-Wsometimes-uninitialized','-Wconditional-uninitialized']
        extoptions['extra_compile_args']+=['-Wuninitialized',
                                           '-Wswitch','-Werror','-Wno-missing-braces']
        print(pkgdata)
    else:
        if sys.version_info < (3, 0, 0):
            raise RuntimeError("Windows on Python earlier than v3 unsupported.")

        warnings.warn("I'm detecting you're running windows."
                      "You might want to modify "
                      "the 'setup.py' script to use appropriate paths")

        # The layout i have here is an ..\lcb-winbuild, in which there are subdirs
        # called 'x86' and 'x64', for x86 and x64 architectures. The default
        # 'nmake install' on libcouchbase will install them to 'deps'
        bit_type = platform.architecture()[0]
        lcb_root = os.path.join(os.path.pardir, 'lcb-winbuild')

        if bit_type.startswith('32'):
            lcb_root = os.path.join(lcb_root, 'x86')
        else:
            lcb_root = os.path.join(lcb_root, 'x64')

        lcb_root = os.path.join(lcb_root, 'deps')

        extoptions['libraries'] = ['libcouchbase']
        if debug_symbols:
            extoptions['extra_compile_args'] += ['/Zi', '/DEBUG', '/O0']
            extoptions['extra_link_args'] += ['/DEBUG', '-debug']
        extoptions['library_dirs'] = [os.path.join(lcb_root, 'lib')]
        extoptions['include_dirs'] = [os.path.join(lcb_root, 'include')]
        extoptions['define_macros'] = [('_CRT_SECURE_NO_WARNINGS', 1)]
        pkgdata[couchbase_core] = ['libcouchbase.dll']

    extoptions['extra_compile_args']+=['-DPYCBC_LCB_API={}'.format(PYCBC_LCB_API)]
    extoptions.update(get_sources())
    return extoptions, pkgdata


class CBuildInfo:
    def __init__(self, cmake_base=None):
        self.setbase(cmake_base)
        self.cfg="Release"
        self.pkg_data_dir=os.path.join(couchbase_core)
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
        default = ['libcouchbase.so.6']
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

    def get_rpaths(self, cfg):
        result= [{'Darwin': '@loader_path', 'Linux': '$ORIGIN'}.get(platform.system(), "$ORIGIN"),
                 os.path.join(*self.lcb_pkgs_srcs()[cfg])]
        print("got rpaths {}".format(result))
        return result

    def get_lcb_dirs(self):
        lcb_dbg_build = os.path.join(*(self.base + ["install", "lib", "Debug"]))
        lcb_build = os.path.join(*(self.base + ["install", "lib", "Release"]))
        lib_dirs = [lcb_dbg_build, lcb_build]
        return lib_dirs


class LazyCommandClass(dict):
    """
    Lazy command class that defers operations requiring given cmdclass until
    they've actually been downloaded and installed by setup_requires.
    """
    def __init__(self, cmdclass_real):
        super(LazyCommandClass, self).__init__()
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


class CBuildCommon(build_ext):
    @classmethod
    def setup_build_info(cls, extoptions, pkgdata):
        cls.info = CBuildInfo()
        cls.info.pkgdata = pkgdata
        cls.info.pkg_data_dir = os.path.join(os.path.abspath("."), couchbase_core)
        pkgdata['couchbase'] = list(cls.info.lcb_pkgs_strlist())
        extoptions['library_dirs'] = [cls.info.pkg_data_dir] + extoptions.get('library_dirs', [])

    def build_extension(self, ext):
        self.init_info_and_rpaths(ext)
        self.prep_build(ext)
        self.add_inc_and_lib_bundled(ext, self.get_lcb_api_flags())
        build_ext.build_extension(self, ext)

    def prep_build(self, ext):
        pass

    def init_info_and_rpaths(self, ext):
        self.ssl_config = gen_config.gen_config(self.build_temp, couchbase_core=couchbase_core)
        self.info.setbase(self.build_temp)
        self.info.cfg = self.cfg_type()
        self.compiler.add_include_dir(os.path.join(*self.info.base+["install","include"]))
        self.compiler.add_library_dir(os.path.join(*self.info.base+["install","lib",self.cfg_type()]))
        if sys.platform == 'darwin':
            warnings.warn('Adding /usr/local to lib search path for OS X')
            self.compiler.add_library_dir('/usr/local/lib')
            self.compiler.add_include_dir('/usr/local/include')
        self.add_rpaths(ext)

    def add_rpaths(self, ext=None, extoptions=None):
        rpaths=self.info.get_rpaths(self.cfg_type())
        if platform.system() != 'Windows':
            if self.compiler:
                try:
                    existing_rpaths = self.compiler.runtime_library_dirs
                    self.compiler.set_runtime_library_dirs(rpaths + existing_rpaths)
                except:
                    pass
            for rpath in rpaths:
                if self.compiler:
                    self.compiler.add_runtime_library_dir(rpath)
                linker_arg='-Wl,-rpath,' + rpath
                ext.runtime_library_dirs=(ext.runtime_library_dirs if ext.runtime_library_dirs else [])+[rpath]
                ext.extra_link_args+=[linker_arg]
                (extoptions['extra_link_args'] if extoptions else ext.extra_link_args if ext else []).insert(0,linker_arg)

    def cfg_type(self):
        return 'Debug' if self.debug else 'Release'

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

    def add_inc_and_lib_bundled(self, ext, lcb_api_flags):
        from distutils.ccompiler import CCompiler
        ext.extra_compile_args += lcb_api_flags
        compiler = self.compiler  # type: CCompiler
        lcb_include = os.path.join(self.build_temp, "install", "include")
        try:
            compiler.set_include_dirs([lcb_include]+compiler.include_dirs)
        except:
            compiler.add_include_dirs([lcb_include])
        lib_dirs = [self.info.pkg_data_dir] + self.info.get_lcb_dirs()
        try:
            existing_lib_dirs = compiler.library_dirs
            compiler.set_library_dirs(lib_dirs + existing_lib_dirs)
        except:
            compiler.add_library_dirs(lib_dirs)

    def get_pycbc_lcb_api(self):
        return os.getenv("PYCBC_LCB_API",
                         BUILD_CFG.get('comp_options', {}).get('PYCBC_LCB_API', None))

    def get_lcb_api_flags(self):
        pycbc_lcb_api=self.get_pycbc_lcb_api()
        return ['-DPYCBC_LCB_API={}'.format(pycbc_lcb_api)] if pycbc_lcb_api else []


class install_headers(install_headers_orig):
    def run(self):
        headers = self.distribution.headers or []
        for header in headers:
            dst = os.path.join(self.install_dir, os.path.dirname(header))
            self.mkpath(dst)
            (out, _) = self.copy_file(header, dst)
            self.outfiles.append(out)


def get_plat_code():
    plat = sys.platform.lower()
    substitutions = {'win': r'^win.*$'}
    for target, pattern in substitutions.items():
        plat = re.compile(pattern).sub(target, plat)
    return plat


build_type = os.getenv("PYCBC_BUILD",
                       {"Windows": "CMAKE_HYBRID", "Darwin": "CMAKE_HYBRID", "Linux": "CMAKE_HYBRID"}.get(platform.system(),
                                                                                                   "CMAKE_HYBRID"))


