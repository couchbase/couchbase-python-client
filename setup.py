#!/usr/bin/env python
import logging
import os
import couchbase_version

from cbuild_config import get_ext_options, couchbase_core, build_type
from cmodule import gen_distutils_build
from cmake_build import gen_cmake_build

try:
    if os.environ.get('PYCBC_NO_DISTRIBUTE'):
        raise ImportError()

    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

import os
import sys
import platform

lcb_min_version = (2, 9, 0)

try:
    from lcb_version import get_lcb_min_version
    lcb_min_version=get_lcb_min_version()
except:
    lcb_min_version=(2,9,0)
if not os.path.exists("build"):
    os.mkdir("build")

with open("build/lcb_min_version.h", "w+") as LCB_MIN_VERSION:
    LCB_MIN_VERSION.write('\n'.join(
        ["#define LCB_MIN_VERSION 0x{}".format(''.join(map(lambda x: "{0:02d}".format(x), lcb_min_version))),
         '#define LCB_MIN_VERSION_TEXT "{}"'.format('.'.join(map(str, lcb_min_version))),
         '#define PYCBC_PACKAGE_NAME "{}"'.format(couchbase_core)]))

try:
    couchbase_version.gen_version()
except couchbase_version.CantInvokeGit:
    pass

pkgversion = couchbase_version.get_version()


def handle_build_type_and_gen_deps():
    cmake_build = build_type in ['CMAKE', 'CMAKE_HYBRID']
    print("Build type: {}, cmake:{}".format(build_type, cmake_build))
    general_requires = open('requirements.txt').readlines()
    extoptions, pkgdata=get_ext_options()

    if cmake_build:
        e_mods, extra_requires, cmdclass = gen_cmake_build(extoptions, pkgdata)
        general_requires += extra_requires
    else:
        print("Legacy build")
        e_mods, cmdclass = gen_distutils_build(extoptions, pkgdata)

    setup_kw = {'ext_modules': e_mods}
    logging.error(setup_kw)

    setup_kw['setup_requires'] = general_requires
    setup_kw['install_requires'] = general_requires
    setup_kw['cmdclass'] = cmdclass
    setup_kw['package_data'] = pkgdata
    setup_kw['eager_resources'] = pkgdata
    return setup_kw


setup_kw = handle_build_type_and_gen_deps()

packages = {
    'acouchbase',
    'couchbase',
    couchbase_core,
    'couchbase_v2',
    couchbase_core+'.views',
    couchbase_core+'.iops',
    couchbase_core+'.asynchronous',
    'couchbase_v2.views',
    'couchbase_v2.iops',
    'couchbase_v2.asynchronous',
    'couchbase_v2.tests',
    'couchbase_v2.tests.cases',
    'gcouchbase',
    'txcouchbase',
    'acouchbase',
}.union({
            'acouchbase.tests',
            'acouchbase.py34only'
        } if sys.version_info >= (3, 4) else set())

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
    python_requires=(">=3" if platform.system().lower().startswith("win") else ">=2.7"),
    packages = list(packages),
    tests_require=['utilspie','nose', 'testresources>=0.2.7', 'basictracer==2.2.0'],
    test_suite='couchbase_tests.test_sync',
    **setup_kw
)
