import sys

if sys.version_info >= (3, 4):
    import acouchbase.py34only.iterator

    sys.modules['acouchbase.iterator'] = acouchbase.py34only.iterator
    if sys.version_info < (3,7):
        import asyncio
        acouchbase.py34only.iterator.AioBase.__aiter__=asyncio.coroutine(acouchbase.py34only.iterator.AioBase.__aiter__)