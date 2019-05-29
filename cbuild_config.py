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
import os.path
import platform
import re
import sys
import warnings


def get_json_build_cfg():
    with open("cbuild_cfg.json") as JSONFILE:
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
        extoptions['extra_compile_args'] += ['-Wno-strict-prototypes', '-fPIC']
        extoptions['libraries'] = ['couchbase']
        if debug_symbols:
            extoptions['extra_compile_args'] += ['-O0', '-g3']
            extoptions['extra_link_args'] += ['-O0', '-g3']
        if sys.platform == 'darwin':
            warnings.warn('Adding /usr/local to search path for OS X')
            extoptions['library_dirs'] = ['/usr/local/lib',
                                          '/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/10.0.0/lib/darwin/']
            extoptions['include_dirs'] = ['/usr/local/include']
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

