#!/usr/bin/env python
import logging
import os.path
import os
import couchbase_version

from cbuild_config import get_ext_options, couchbase_core
from cmodule import gen_cmodule

try:
    if os.environ.get('PYCBC_NO_DISTRIBUTE'):
        raise ImportError()

    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

from distutils.command.install_headers import install_headers as install_headers_orig

import os
import sys
import platform

from setuptools.command.build_ext import build_ext

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

# Dummy dependency to prevent installation of Python < 3 package on Windows.


pip_not_on_win_python_lt_3 = ["pip>=20.0; (sys_platform == 'win32' and python_version <= '2.7')"]


build_type = os.getenv("PYCBC_BUILD",
                       {"Windows": "CMAKE_HYBRID", "Darwin": "CMAKE_HYBRID", "Linux": "CMAKE_HYBRID"}.get(platform.system(),
                                                                                                   "CMAKE_HYBRID"))


class install_headers(install_headers_orig):
    def run(self):
        headers = self.distribution.headers or []
        for header in headers:
            dst = os.path.join(self.install_dir, os.path.dirname(header))
            self.mkpath(dst)
            (out, _) = self.copy_file(header, dst)
            self.outfiles.append(out)


def gen_cmake_build(extoptions, pkgdata):
    from cmake_build import CMakeExtension, CMakeBuild, CMakeBuildInfo

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
    CMakeBuild.info = CMakeBuildInfo()
    CMakeBuild.info.pkgdata = pkgdata
    CMakeBuild.info.pkg_data_dir = os.path.join(os.path.abspath("."), couchbase_core)
    pkgdata['couchbase'] = list(CMakeBuild.info.lcb_pkgs_strlist())
    extoptions['library_dirs']=[CMakeBuild.info.pkg_data_dir]+extoptions.get('library_dirs',[])
    e_mods = [CMakeExtension(str(couchbase_core+'._libcouchbase'), '', **extoptions)]
    return e_mods, CMakeBuild.requires(), LazyCommandClass(CMakeBuild)


def gen_distutils_build(extoptions,pkgdata):
    b_ext = build_ext
    e_mods = [gen_cmodule(extoptions)]
    cmdclass = {'install_headers': install_headers, 'build_ext': b_ext}
    return e_mods, cmdclass


def handle_build_type_and_gen_deps():
    cmake_build = build_type in ['CMAKE', 'CMAKE_HYBRID']
    print("Build type: {}, cmake:{}".format(build_type, cmake_build))
    general_requires = ['pyrsistent', "enum34; python_version < '3.5'"]
    extoptions, pkgdata=get_ext_options()

    if cmake_build:
        e_mods, extra_requires, cmdclass = gen_cmake_build(extoptions,pkgdata)
        general_requires += extra_requires
    else:
        print("Legacy build")
        e_mods, cmdclass = gen_distutils_build(extoptions,pkgdata)

    setup_kw = {'ext_modules': e_mods}
    logging.error(setup_kw)

    typing_requires = (['typing'] if sys.version_info < (3, 7) else [])

    exec_requires = typing_requires + general_requires
    conan_build = os.environ.get("PYCBC_CONAN_BUILD")
    conan_deps = ['conan'] if conan_build else []
    conan_and_cmake_deps = ((['scikit-build', 'cmake>=3.0.2'] + conan_deps) if
                            cmake_build and sys.platform.startswith('darwin') else [])
    setup_kw['setup_requires'] = exec_requires + conan_and_cmake_deps
    setup_kw['install_requires'] = exec_requires + pip_not_on_win_python_lt_3
    setup_kw['cmdclass']=cmdclass
    setup_kw['package_data']=pkgdata
    setup_kw['eager_resources']=pkgdata
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
