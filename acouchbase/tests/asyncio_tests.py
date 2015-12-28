# SyntaxError will trigger if yield or async is not supported
# ImportError will fail for python 3.3 because asyncio does not exist

try:
    from py34only import CouchbaseBeerTest, CouchbaseDefaultTest
except ImportError:
    pass
except SyntaxError:
    pass

try:
    from py35only import CouchbasePy35Test
except ImportError:
    pass
except SyntaxError:
    pass
