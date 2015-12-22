try:
    from py34only import CouchBaseTest
except SyntaxError:
    pass

try:
    from py35only import CouchBasePy35Test
except SyntaxError:
    pass
