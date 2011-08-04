# chardet's setup.py
from distutils.core import setup
setup(
    name = "couchbase",
    packages = ["couchbase", "couchbase/httplib2"],
    version = "0.0.1",
    description = "Couchbase Python SDK",
    author = "Couchbase Inc",
    author_email = "info@couchbase.com",
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
Couchbase Python Library
This version requires Python 2.6 or later
"""
)
