# Initialize distribute
import distribute_setup
# Assume they're using a virtual env, and force installation if so -- this is
# something of a hack but distribute is a Good Thing anyway so don't quibble
# for now
try:
    import sys
    import os
    _virtual_env = os.environ.get('VIRTUAL_ENV', "__NONE__")
    _lib_path = [p for p in sys.path if p.startswith(_virtual_env) and
                 "site-packages" in p and not
                 (p.endswith('egg') or p.endswith('egg-info'))]
    # now _lib_path should be a site-packages or similar directory
    _to_dir = _lib_path[0]
    _no_fake = True
except:
    _to_dir = os.curdir
    _no_fake = False

distribute_setup.use_setuptools(no_fake=_no_fake, to_dir=_to_dir)

# Now use `setuptools`, which should be wrapped by distribute
from setuptools import setup
from setuptools import Extension
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
