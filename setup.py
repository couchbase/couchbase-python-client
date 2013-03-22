from distutils.core import setup
from distutils.extension import Extension


setup(
  name = 'Couchbase Python SDK',
  ext_modules = [Extension("lcb", ["lcb.c"], libraries=['couchbase'])]
)
