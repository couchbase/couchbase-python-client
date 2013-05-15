#!/usr/bin/env python
import sys
import os.path
import platform
import warnings

from distutils.core import setup, Extension

extoptions = {}

LCB_NAME = None
if sys.platform != 'win32':
    extoptions['libraries'] = ['couchbase']
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


SOURCEMODS = (
        'argument',
        'exceptions',
        'ext',
        'result',
        'opresult',
        'callbacks',
        'convert',
        'connection',
        'store',
        'constants',
        'multiresult',
        'miscops',
        'numutil',
        'oputil',
        'get',
        'arithmetic',
        'http',
        'htresult'
        )

extoptions['sources'] = [ os.path.join("src", m + ".c") for m in SOURCEMODS ]
module = Extension('couchbase._libcouchbase', **extoptions)

setup(
    name = 'couchbase', version = '0.10',
    url="https://github.com/couchbase/couchbase-python-client",
    author="Couchbase, Inc.",
    author_email="mark.nunberg@couchbase.com",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"],
    ext_modules = [module],
    packages = ['couchbase']
)
