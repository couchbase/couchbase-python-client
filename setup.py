from distutils.core import setup
import subprocess

def get_version():
    try:
        p = subprocess.Popen('git describe', stdout=subprocess.PIPE, shell=True)
        version = p.communicate()[0].strip()
    except:
        version = ''
    return version


setup(
    name = "couchbase-python",
    version = get_version(),
    description = "Couchbase Python SDK",
    author = "Couchbase Inc",
    author_email = "info@couchbase.com",
    packages = ["couchbase", "couchbase/httplib2", "couchbase/utils", "couchbase/migrator"],
    url = "http://couchbase.org/",
    download_url = "http://.../pysdk.tar.gz",
    license = "LICENSE.txt",
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
    long_description = open('README.txt').read(),
)
