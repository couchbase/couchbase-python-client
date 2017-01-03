#!/usr/bin/env python
import sys
import os.path
import os
import platform
import warnings
import couchbase_version

try:
    if os.environ.get('PYCBC_NO_DISTRIBUTE'):
        raise ImportError()

    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

extoptions = {}
pkgdata = {}
pkgversion = None

try:
    couchbase_version.gen_version()
except couchbase_version.CantInvokeGit:
    pass

pkgversion = couchbase_version.get_version()


LCB_NAME = None
if sys.platform != 'win32':
    extoptions['libraries'] = ['couchbase']
    if sys.platform == 'darwin':
        warnings.warn('Adding /usr/local to search path for OS X')
        extoptions['library_dirs'] = ['/usr/local/lib']
        extoptions['include_dirs'] = ['/usr/local/include']
else:
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
    #extoptions['extra_compile_args'] = ['/Zi']
    #extoptions['extra_link_args'] = ['/DEBUG']
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

setup(
    name = 'couchbase',
    version = pkgversion,
    url="https://github.com/couchbase/couchbase-python-client",
    author="Couchbase, Inc.",
    author_email="mark.nunberg@couchbase.com",
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
        'couchbase.async',
        'couchbase.tests',
        'couchbase.tests.cases',
        'gcouchbase',
        'txcouchbase',
        'acouchbase'
    ],
    package_data = pkgdata,
    tests_require = [ 'nose', 'testresources>=0.2.7' ],
    test_suite = 'couchbase.tests.test_sync',
    **setup_kw
)
