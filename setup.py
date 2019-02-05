#!/usr/bin/env python
import sys
import os.path
import os
import platform
import warnings
import couchbase_version
import pip

try:
    if os.environ.get('PYCBC_NO_DISTRIBUTE'):
        raise ImportError()

    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

import re
extoptions = {}
pkgdata = {}
pkgversion = None

lcb_min_version = (2, 9, 0)

try:
    from lcb_version import get_lcb_min_version
    lcb_min_version=get_lcb_min_version()
except:
    lcb_min_version=(2,9,0)
if not os.path.exists("build"):
    os.mkdir("build")

with open("build/lcb_min_version.h", "w+") as LCB_MIN_VERSION:
    LCB_MIN_VERSION.writelines(
        ["#define LCB_MIN_VERSION 0x{}\n".format(''.join(map(lambda x: "{0:02d}".format(x), lcb_min_version))),
         '#define LCB_MIN_VERSION_TEXT "{}"'.format('.'.join(map(str, lcb_min_version)))])

try:
    couchbase_version.gen_version()
except couchbase_version.CantInvokeGit:
    pass

pkgversion = couchbase_version.get_version()


LCB_NAME = None
extoptions['extra_compile_args'] = []
extoptions['extra_link_args'] = []


def boolean_option(flag):
    return "-D{}={}".format(flag,os.environ.get(flag))


COMP_OPTION_PREFIX = "PYCBC_COMP_OPT_"


def comp_option(flag):
    return "-{}={}".format(flag.replace(COMP_OPTION_PREFIX, ""), os.environ.get(flag))


COMP_OPTION_BOOL_PREFIX = "PYCBC_COMP_OPT_BOOL_"


def comp_option_bool(flag):
    return "-{}".format(flag.replace(COMP_OPTION_BOOL_PREFIX, ""))


CLANG_SAN_OPTIONS={"address":"lsan","undefined":"ubsan"}

CLANG_SAN_PREFIX = "PYCBC_SAN_OPT_"

def comp_clang_san_option(flag):
    san_option = flag.replace(CLANG_SAN_PREFIX, "")
    fsanitize_statement = "-fsanitize={}".format(san_option)
    extoptions['extra_link_args']+=["{}".format(fsanitize_statement)]
    return fsanitize_statement


def comp_option_pattern(prefix):
    return re.escape(prefix) + ".*"


comp_flags = {"PYCBC_STRICT":boolean_option,
              "PYCBC_TABBED_CONTEXTS_ENABLE": boolean_option,
              "PYCBC_REF_ACCOUNTING": boolean_option,
              "PYCBC_TRACING_DISABLE": boolean_option, "PYCBC_DEBUG": boolean_option,
              "PYCBC_CRYPTO_VERSION": boolean_option, comp_option_pattern(COMP_OPTION_PREFIX): comp_option,
              comp_option_pattern(COMP_OPTION_BOOL_PREFIX): comp_option_bool,
              comp_option_pattern(CLANG_SAN_PREFIX):comp_clang_san_option}

debug_symbols = len(set(os.environ.keys()) & {"PYCBC_DEBUG", "PYCBC_DEBUG_SYMBOLS"}) > 0
comp_arg_additions = (action(actual_flag) for flag, action in comp_flags.items() for actual_flag in os.environ.keys() if
                      re.match(flag, actual_flag))
extoptions['extra_compile_args'] += comp_arg_additions
if sys.platform != 'win32':
    extoptions['libraries'] = ['couchbase']
    if debug_symbols:
        extoptions['extra_compile_args'] += ['-O0', '-g3']
        extoptions['extra_link_args'] += ['-O0', '-g3']
    if sys.platform == 'darwin':
        warnings.warn('Adding /usr/local to search path for OS X')
        extoptions['library_dirs'] = ['/usr/local/lib']
        extoptions['include_dirs'] = ['/usr/local/include']

else:
    if sys.version_info<(3,0,0):
        if pip.__version__<"9.0.0":
            raise pip.exceptions.InstallationError("Windows on Python earlier than v3 unsupported.")

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
    ## Enable these lines for debug builds
    if debug_symbols:
        extoptions['extra_compile_args'] += ['/Zi','/DEBUG','/O0']
        extoptions['extra_link_args'] += ['/DEBUG','-debug']
    extoptions['library_dirs'] = [os.path.join(lcb_root, 'lib')]
    extoptions['include_dirs'] = [os.path.join(lcb_root, 'include')]
    extoptions['define_macros'] = [('_CRT_SECURE_NO_WARNINGS', 1)]
    pkgdata['couchbase'] = ['libcouchbase.dll']


SOURCEMODS = [
        'exceptions',
        'ext',
        'result',
        'opresult',
        'callbacks',
        'cntl',
        'convert',
        'bucket',
        'store',
        'constants',
        'multiresult',
        'miscops',
        'typeutil',
        'oputil',
        'get',
        'counter',
        'http',
        'htresult',
        'ctranscoder',
        'crypto',
        'observe',
        'iops',
        'connevents',
        'pipeline',
        'views',
        'n1ql',
        'fts',
        'ixmgmt'
        ]

if platform.python_implementation() != 'PyPy':
    extoptions['sources'] = [ os.path.join("src", m + ".c") for m in SOURCEMODS ]
    module = Extension('couchbase._libcouchbase', **extoptions)
    setup_kw = {'ext_modules': [module]}
else:
    warnings.warn('The C extension libary does not work on PyPy. '
            'You should install the couchbase_ffi module. Installation of this '
            'module will continue but will be unusable without couchbase_ffi')
    setup_kw = {}

cmake_build=os.environ.get("PYCBC_CMAKE_BUILD")

# Dummy dependency to prevent installation of Python < 3 package on Windows.

pip_not_on_win_python_lt_3 = (
    ["pip>=9.0; (sys_platform != 'win32' and python_version >= '2.7') or (python_version >= '3.0')"]
    if pip.__version__ >= "9.0.0"
    else [])

conan_and_cmake_deps = (['conan', 'cmake>=3.0.2'] if
                        cmake_build and sys.platform.startswith('darwin') else [])

setup(
    name = 'couchbase',
    version = pkgversion,
    url="https://github.com/couchbase/couchbase-python-client",
    author="Couchbase, Inc.",
    author_email="PythonPackage@couchbase.com",
    license="Apache License 2.0",
    description="Python Client for Couchbase",
    long_description=open("README.rst", "r").read(),
    keywords=["couchbase", "nosql", "pycouchbase", "libcouchbase"],

    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"],

    packages = [
        'acouchbase',
        'couchbase',
        'couchbase.views',
        'couchbase.iops',
        'couchbase.asynchronous',
        'couchbase.tests',
        'couchbase.tests.cases',
        'gcouchbase',
        'txcouchbase',
        'acouchbase',
    ] + ([
        'acouchbase.tests',
        'acouchbase.py34only'
    ] if sys.version_info >= (3, 4) else []),
    package_data=pkgdata,
    setup_requires=['typing'] + conan_and_cmake_deps,
    install_requires=['typing'] + pip_not_on_win_python_lt_3,
    tests_require=['nose', 'testresources>=0.2.7', 'basictracer==2.2.0'],
    test_suite='couchbase.tests.test_sync',
    **setup_kw
)
