from couchbase_tests.base import MockTestCase
import couchbase_core.subdocument as SD


class XattrTest(MockTestCase):
    def test_xattrs_basic(self):
        cb = self.cb
        k = self.gen_key('xattrs')
        cb.upsert(k, {})

        # Try to upsert a single xattr
        rv = cb.mutate_in(k, SD.upsert('my.attr', 'value',
                                       xattr=True,
                                       create_parents=True))
        self.assertTrue(rv.success)

        body = cb.get(k)
        self.assertFalse('my' in body.value)
        self.assertFalse('my.attr' in body.value)

        # Try using lookup_in
        rv = cb.retrieve_in(k, 'my.attr')
        self.assertFalse(rv.exists('my.attr'))

        # Finally, use lookup_in with 'xattrs' attribute enabled
        rv = cb.lookup_in(k, SD.get('my.attr', xattr=True))
        self.assertTrue(rv.exists('my.attr'))
        self.assertEqual('value', rv['my.attr'])