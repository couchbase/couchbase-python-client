from distutils.core import setup, Command
from distutils.extension import Extension
from distutils.version import StrictVersion
import os
import sys

# Use Cython if available.
try:
    from Cython import __version__ as cython_version
    from Cython.Build import cythonize
except ImportError:
    print('importerror')
    cythonize = None

# When building from a repo, Cython is required.
if os.path.exists("MANIFEST.in"):
    print("MANIFEST.in found, presume a repo, cythonizing...")
    if not cythonize:
        print(
            "Error: Cython.Build.cythonize not found. "
            "Cython is required to build from a repo.")
        sys.exit(1)
    elif StrictVersion(cython_version) <= StrictVersion("0.18"):
        print("Error: You need a Cython version newer than 0.18")
        sys.exit(1)
    ext_modules = cythonize([
        Extension(
            'couchbase/libcouchbase', ['couchbase/libcouchbase.pyx'],
            libraries=['couchbase'])])
# If there's no manifest template, as in an sdist, we just specify .c files.
else:
    ext_modules = [
        Extension(
            'couchbase/libcouchbase', ['couchbase/libcouchbase.c'],
            libraries=['couchbase'])]


setup(
    name="couchbase",
    version="0.9",
    url="https://github.com/couchbase/couchbase-python-client",
    author="Volker Mische",
    author_email="volker@couchbase.com",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"],
    ext_modules=ext_modules,
    packages=['couchbase']
)
