from couchbase_tests.base import ConnectionTestCase, CollectionTestCase
from couchbase_core._libcouchbase import FMT_UTF8
import couchbase_core.subdocument as SD
import couchbase.exceptions as E


class SubdocTest(CollectionTestCase):
    def setUp(self):
        super(SubdocTest, self).setUp()
        cb = self.cb
        k = self.gen_key('sd_precheck')
        try:
            cb.lookup_in(k, 'pth')
        except (E.NotSupportedException, E.UnknownCommandException):
            self.skipTest('Subdoc not supported on this server version')
        except E.CouchbaseException:
            pass

    def test_lookup_in(self):
        cb = self.cb
        # Create the document
        key = self.gen_key('sdget')
        cb.upsert(key, {
            'path1': 'value1'
        })

        result = cb.get(key, project=['path1'])
        self.assertEqual((0, 'value1'), result.get(0))
        self.assertEqual((0, 'value1'), result.get('path1'))
        self.assertEqual('value1', result[0])
        self.assertEqual('value1', result['path1'])
        self.assertTrue(result.cas)

        # Try when path is not found
        rv = cb.retrieve_in(key, 'path2')
        self.assertRaises(E.PathNotFoundException, rv.__getitem__, 0)
        self.assertRaises(E.PathNotFoundException, rv.__getitem__, 'path2')

        # Try when there is a mismatch
        self.assertRaises(E.SubdocPathMismatchException,
                          cb.retrieve_in, key, 'path1[0]')

        # Try existence
        result = cb.lookup_in(key, SD.exists('path1'))
        self.assertTrue(result.exists('path1'))
        self.assertTrue(result.exists(0))

        # Not found
        result = cb.lookup_in(key, SD.exists('p'))
        self.assertEqual(E.PathNotFoundException.CODE, result.get(0)[0])

        # Ensure that we complain about a missing path
        self.assertRaises((IndexError, KeyError), result.get, 33)
        self.assertRaises((IndexError, KeyError), result.get, 'non-requested')

        # Test with quiet
        result = cb.lookup_in(key, SD.exists('p'), quiet=True)
        self.assertFalse(result.exists('p'))
        self.assertFalse(result.exists(0))

        # Insert a non-JSON document
        bkey = self.gen_key('sdget_nonjson')
        cb.upsert(bkey, 'value', format=FMT_UTF8)
        self.assertRaises(E.DocumentNotJsonException,
                          cb.lookup_in, bkey, SD.exists('path'))

        # Empty paths fail for get_in
        #self.assertRaises(E.SubdocEmptyPathException,
        #                  cb.retrieve_in, key, '')

        # Try on non-existing document. Should fail
        self.assertRaises(E.DocumentNotFoundException,
                          cb.retrieve_in, 'non-exist', 'path')

    def test_mutate_in(self):
        cb = self.cb
        key = self.gen_key('sdstore_upsert')
        cb.upsert(key, {})

        cb.mutate_in(key, SD.upsert('newDict', ['hello']))
        result = cb.retrieve_in(key, 'newDict')
        self.assertEqual(['hello'], result[0])

        # Create deep path without create_parents
        self.assertRaises(E.PathNotFoundException,
                          cb.mutate_in, key,
                          SD.upsert('path.with.missing.parents', 'value'))

        # Create deep path using create_parents
        cb.mutate_in(key,
                     SD.upsert('new.parent.path', 'value', create_parents=True))
        result = cb.retrieve_in(key, 'new.parent')
        self.assertEqual('value', result[0]['path'])

        # Test CAS operations
        self.assertTrue(result.cas)
        self.assertRaises(E.DocumentExistsException, cb.mutate_in,
                          key, SD.upsert('newDict', None), cas=result.cas+1)

        # Try it again, using the CAS
        result2 = cb.mutate_in(key, SD.upsert('newDict', {}), cas=result.cas)
        self.assertNotEqual(result.cas, result2.cas)

        # Test insert, should fail
        self.assertRaises(E.PathExistsException, cb.mutate_in,
                          key, SD.insert('newDict', {}))

        # Test insert on new path, should succeed
        cb.mutate_in(key, SD.insert('anotherDict', {}))
        self.assertEqual({}, cb.retrieve_in(key, 'anotherDict')[0])

        # Test replace, should not fail
        cb.mutate_in(key, SD.replace('newDict', {'Hello': 'World'}))
        self.assertEqual('World', cb.retrieve_in(key, 'newDict')[0]['Hello'])

        # Test replace with missing value, should fail
        self.assertRaises(E.PathNotFoundException,
                          cb.mutate_in, key, SD.replace('nonexist', {}))

        # Test with empty string (should be OK)
        cb.mutate_in(key, SD.upsert('empty', ''))
        self.assertEqual('', cb.retrieve_in(key, 'empty')[0])

        # Test with null (None). Should be OK
        cb.mutate_in(key, SD.upsert('null', None))
        self.assertEqual(None, cb.retrieve_in(key, 'null')[0])

        # Test with empty path. Should throw some kind of error?
        self.assertRaises(
            (E.SubdocPathInvalidException),
            cb.mutate_in, key, SD.upsert('', {}))

        cb.mutate_in(key, SD.upsert('array', [1, 2, 3]))
        self.assertRaises(E.SubdocPathMismatchException, cb.mutate_in, key,
                          SD.upsert('array.newKey', 'newVal'))
        self.assertRaises(E.SubdocPathInvalidException, cb.mutate_in, key,
                          SD.upsert('array[0]', 'newVal'))
        self.assertRaises(E.PathNotFoundException, cb.mutate_in, key,
                          SD.upsert('array[3].bleh', 'newVal'))

    def test_counter_in(self):
        cb = self.cb
        key = self.gen_key('sdcounter')
        cb.upsert(key, {})

        rv = cb.mutate_in(key, (SD.counter('counter', 100),))
        self.assertTrue(rv.success)
        self.assertFalse(rv.cas == 0)
        self.assertEqual(100, rv.content_as[int](0))

        self.assertRaises(E.SubdocBadDeltaException, cb.mutate_in, key,
                          (SD.counter('not_a_counter', 'blah'),))

        # Do an upsert
        cb.mutate_in(key, (SD.upsert('not_a_counter', 'blah'),))

        self.assertRaises(E.SubdocPathMismatchException, cb.mutate_in, key,
                          (SD.counter('not_a_counter', 25),))

        self.assertRaises(E.PathNotFoundException, cb.mutate_in, key,
                          (SD.counter('path.to.newcounter', 99),))
        rv = cb.mutate_in(key,
                          (SD.counter('path.to.newcounter', 99, create_parents=True),))
        self.assertEqual(99, rv.content_as[int](0))

        # Increment first counter again
        rv = cb.mutate_in(key, (SD.counter('counter', -25),))
        self.assertEqual(75, rv.content_as[int](0))

        self.assertRaises(ValueError, SD.counter, 'counter', 0)

    def test_multi_lookup(self):
        cb = self.cb
        key = self.gen_key('sdmlookup')
        cb.upsert(key, {
            'field1': 'value1',
            'field2': 'value2',
            'array': [1, 2, 3],
            'boolean': False
        })

        rvs = cb.lookup_in(
            key, SD.get('field1'), SD.exists('field2'), SD.exists('field3'),
            quiet=True
        )


        # LCB used to be consider success to be true only if it found
        # _all_ the paths.  Now, however, lcb returns success if it found
        # any of them, so...
        self.assertTrue(rvs.success)
        self.assertEqual(3, rvs.result_count)

        self.assertEqual((0, 'value1'), rvs.get(0))
        self.assertEqual((0, 'value1'), rvs.get('field1'))
        self.assertEqual('value1', rvs[0])
        self.assertEqual('value1', rvs['field1'])

        self.assertEqual((0, None), rvs.get(1))
        self.assertEqual((0, None), rvs.get('field2'))
        self.assertEqual(None, rvs[1])
        self.assertEqual(None, rvs['field2'])

        self.assertTrue(rvs.exists('field2'))
        self.assertTrue(rvs.exists(1))
        self.assertTrue(1 in rvs)
        self.assertTrue('field2' in rvs)

        self.assertEqual((E.PathNotFoundException.CODE, None),
                         rvs.get('field3'))
        self.assertEqual((E.PathNotFoundException.CODE, None),
                         rvs.get(2))
        self.assertFalse(rvs.exists('field3'))
        self.assertFalse(rvs.exists(2))

        def _getix(rv_, ix):
            return rv_[ix]

        self.assertRaises(E.PathNotFoundException, _getix, rvs, 2)
        self.assertRaises(E.PathNotFoundException, _getix, rvs, 'field3')
        self.assertFalse(rvs.exists('field3'))

        # See what happens when we mix operations
        self.assertRaises(E.CouchbaseException, cb.lookup_in, key,
                          SD.get('field1'), SD.insert('a', 'b'))

        # Empty path (invalid)
        self.assertRaises(E.CouchbaseException, cb.lookup_in, SD.get(''))

    def test_multi_value(self):
        cb = self.cb
        key = self.gen_key('sdArray')

        cb.upsert(key, {'array': []})
        cb.mutate_in(key, (SD.array_append('array', True),))
        self.assertEqual([True], cb.get(key, project=['array'])[0])

        cb.mutate_in(key, (SD.array_append('array', 1, 2, 3),))
        self.assertEqual([True, 1, 2, 3], cb.get(key, project=['array'])[0])

        cb.mutate_in(key, (SD.array_prepend('array', [42]),))
        self.assertEqual([[42], True, 1, 2, 3], cb.get(key, project=['array'])[0])

    def test_result_iter(self):
        cb = self.cb
        key = self.gen_key('sditer')
        cb.upsert(key, [1, 2, 3])
        vals = cb.retrieve_in(key, '[0]', '[1]', '[2]')
        v1, v2, v3 = vals
        self.assertEqual(1, v1)
        self.assertEqual(2, v2)
        self.assertEqual(3, v3)

        vals = cb.retrieve_in(key, '[0]', '[34]', '[3]')
        # even though 34 isn't there, since it did find the
        # others, lcb says success.
        self.assertTrue(vals.success)
        it = iter(vals)
        self.assertEqual(1, next(it))
        self.assertRaises(E.PathNotFoundException, next, it)

    def test_access_ok(self):
        cb = self.cb
        key = self.gen_key('non-exist')
        try:
            cb.get(key, project=['pth1'], quiet=True)
        except E.DocumentNotFoundException as e:
            rv = e.all_results[key]
            self.assertFalse(rv.access_ok)

        cb.upsert(key, {'hello': 'world'})
        rv = cb.get(key, project=['nonexist'])
        self.assertTrue(rv.success)

    def test_get_count(self):
        cb = self.cb
        key = self.gen_key('get_count')

        cb.upsert(key, [1, 2, 3])
        self.assertEqual(3, cb.get(key, project=['']).content[''][2])

        cb.upsert(key, {'k1': 1, 'k2': 2, 'k3': 3})
        self.assertEqual(3, cb.lookup_in(key, (SD.get_count(''),)).content_as[int](0))

    def test_create_doc(self):
        cb = self.cb
        key = self.gen_key('create_doc')
        cb.mutate_in(key, (SD.upsert('new.path', 'newval'),), upsert_doc=True)
        result= cb.get(key, project=['new.path'])
        self.assertEqual('newval',result.content['new.path'])

        # Check 'insert_doc'

        self.assertRaises(E.DocumentExistsException, cb.mutate_in, key, (SD.upsert('new.path', 'newval'),), insert_doc=True)
        cb.remove(key)

        cb.mutate_in(key, (SD.upsert('new.path', 'newval'),), insert_doc=True)
        self.assertEqual('newval', cb.get(key, project=['new.path']).content['new.path'])
