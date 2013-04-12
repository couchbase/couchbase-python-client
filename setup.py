from distutils.core import setup, Command
from distutils.extension import Extension

try:
    from Cython import __version__ as cython_version

    from distutils.version import StrictVersion

    if StrictVersion(cython_version) <= StrictVersion("0.18"):
        print("You need a Cython version newer than 0.18 to "
              "enable 'cythonize'")
        raise ImportError

    from Cython.Build import cythonize


    class CythonizeCommand(Command):
        description = "Generate .c files out of .pyx"
        user_options = []

        def initialize_options(self):
            pass

        def finalize_options(self):
            pass

        def run(self):
            cythonize("couchbase/libcouchbase.pyx")

    cmdclass = {"cythonize": CythonizeCommand}
except ImportError:
    cmdclass = {}

setup(
    name="couchbase-python-sdk",
    version="0.9",
    url="https://github.com/couchbaselabs/pylibcouchbase",
    author="Volker Mische",
    author_email="volker@couchbase.com",
    license="Apache License 2.0",
    classifiers=["Development Status :: 3 - Alpha",
                 "License :: OSI Approved :: Apache Software License",
                 "Intended Audience :: Developers",
                 "Operating System :: OS Independent",
                 "Programming Language :: Python",
                 "Topic :: Software Development :: Libraries",
                 "Topic :: Software Development :: Libraries :: Python Modules"],
    cmdclass=cmdclass,
    ext_modules=[Extension(
        "couchbase/libcouchbase", ["couchbase/libcouchbase.c"],
        libraries=['couchbase'])
    ],
    packages=['couchbase']
)
