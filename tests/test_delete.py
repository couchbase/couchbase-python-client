from couchbase.exceptions import (KeyExistsError, NotFoundError)
from couchbase.libcouchbase import Connection
from tests.base import CouchbaseTestCase

class ConnectionDeleteTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionDeleteTest, self).setUp()
        self.cb = Connection(self.host, self.port, self.username,
                             self.password, self.bucket_prefix)

    def test_trivial_delete(self):
        """
        Try to delete a key that exists. Ensure that the operation
        succeeds
        """
        cas = self.cb.set('trivial_key', 'value')
        self.assertTrue(cas > 0)
        rv = self.cb.delete('trivial_key')
        self.assertTrue(rv)

    def test_delete_notfound(self):
        """
        Delete a key that does not exist.
        With 'quiet' ensure that it returns false. Without 'quiet', ensure that
        it raises a NotFoundError
        """
        self.cb.delete("foo", quiet = True)
        rv = self.cb.delete("foo", quiet = True)
        self.assertFalse(rv)
        self.assertRaises(NotFoundError, self.cb.delete, 'foo')

    def test_delete_cas(self):
        """
        Delete with a CAS value. Ensure that it returns OK
        """
        cas = self.cb.set('foo', 'bar')
        self.assertTrue(cas > 0)
        rv = self.cb.delete("foo", cas = cas)
        self.assertTrue(rv)

    def test_delete_badcas(self):
        """
        Simple delete with a bad CAS
        """
        self.cb.set('foo', 'bar')
        self.assertRaises(KeyExistsError,
                self.cb.delete, 'foo', cas = 0xdeadbeef)

    def test_delete_list(self):
        """
        Delete passing a list of keys
        """
        kvlist = {}
        num_keys = 5
        for i in range(num_keys):
            kvlist["key_" + str(i)] = str(i)

        rvs = self.cb.set(kvlist)
        self.assertTrue(len(rvs) == num_keys)
        rm_rvs = self.cb.delete(list(rvs.keys()))
        self.assertTrue(len(rm_rvs) == num_keys)

        for k, v in rm_rvs.items():
            self.assertTrue(k in kvlist)
            self.assertTrue(v)

    def test_delete_dict(self):
        """
        Delete passing a dict of key:cas pairs
        """
        kvlist = {}
        num_keys = 5
        for i in range(num_keys):
            kvlist["key_" + str(i)] = str(i)

        rvs = self.cb.set(kvlist)

        # We should just be able to pass it to 'delete'
        rm_rvs = self.cb.delete(rvs)
        for k, v in rm_rvs.items():
            self.assertTrue(v)

    def test_delete_mixed(self):
        """
        Delete with mixed success-error keys.
        Test with mixed found/not-found
        Test with mixed cas-valid/cas-invalid
        """
        self.cb.delete("foo", quiet = True)
        self.cb.set("bar", "a_value")
        # foo does not exit,

        rvs = self.cb.delete(('foo', 'bar'), quiet = True)
        self.assertTrue(rvs['bar'])
        self.assertFalse(rvs['foo'])

        # Now see what happens if we delete those with a bad CAS
        keys = [ "key1", "key2", "key3" ]
        kvs = {}
        for k in keys:
            kvs[k] = "value_" + k
        cas_rvs = self.cb.set(kvs)

        # Ensure set had no errors
        set_errors = []
        for k, v in cas_rvs.items():
            if not v:
                set_errors.append([k, v])
        self.assertTrue(len(set_errors) == 0)

        # Set one to have a bad CAS
        cas_rvs[keys[0]] = 0xdeadbeef
        self.assertRaises(KeyExistsError, self.cb.delete, cas_rvs)



if __name__ == '__main__':
    unittest.main()
