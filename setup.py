# chardet's setup.py
from distutils.core import setup
setup(
    name = "pymembase",
    packages = ["pymembase"],
    version = "0.0.1",
    description = "Membase Python SDK",
    author = "Couchbase Inc",
    author_email = "info@couchbae.com",
    url = "http://couchbase.org/",
    download_url = "http://.../pysdk.tar.gz",
    keywords = ["encoding", "i18n", "xml"],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    long_description = """\
Membase Python Library
his version requires Python 2.6 or later
"""
)
