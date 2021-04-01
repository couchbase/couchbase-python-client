from random import random

from couchbase.exceptions import  PathNotFoundException
from couchbase_tests.base import MockTestCase, CollectionTestCase
import couchbase.subdocument as SD


class XattrTest(CollectionTestCase):
    def test_xattrs_basic(self):
        cb = self.cb
        k = self.gen_key('xattrs' + random().__str__())
        cb.upsert(k, {})

        verbs = ((SD.insert, True), (SD.upsert, True), (SD.replace, False))
        for verb in verbs:
            kwargs = dict(create_parents=True) if verb[1] else {}
            # Operate on a single xattr
            rv = cb.mutate_in(k, [verb[0]('my.attr', 'value',
                                          xattr=True,
                                          **kwargs)])
            self.assertTrue(rv.success)

            body = cb.get(k)
            self.assertFalse('my' in body.content)
            self.assertFalse('my.attr' in body.content)

            # Try using lookup_in
            rv = cb.lookup_in(k, (SD.get('my.attr'),))
            #self.assertRaises(PathNotFoundException, rv.exists, 0)
            self.assertFalse(rv.exists(0))

            # Finally, use lookup_in with 'xattrs' attribute enabled
            rv = cb.lookup_in(k, (SD.get('my.attr', xattr=True),))
            self.assertTrue(rv.exists(0))
            self.assertEqual('value', rv.content_as[str](0))