import sys

if sys.version_info >= (3, 4):
    import acouchbase.py34only.iterator

    sys.modules['acouchbase.iterator'] = acouchbase.py34only.iterator
