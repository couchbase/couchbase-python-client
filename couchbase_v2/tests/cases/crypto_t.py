#
# Copyright 2018, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from unittest import SkipTest

from couchbase._bootstrap import describe_lcb_api
from couchbase.tests.base import ConnectionTestCase
import codecs
import couchbase_v2.exceptions
from couchbase_v2.exceptions import CryptoProviderKeySizeException
from couchbase_core.crypto import CTypesCryptoProvider, InMemoryKeyStore, PythonCryptoProvider
import os
import logging
import couchbase_core._libcouchbase as _LCB


class AESCryptoProvider(CTypesCryptoProvider):
    @staticmethod
    def example_provider(x):
        return os.path.join(os.getcwd(), "..", "libcouchbase", "example", "crypto",
                            x + "." + CTypesCryptoProvider.postfix)

    def __init__(self, keystore):
        super(AESCryptoProvider, self).__init__("openssl_symmetric_provider", "osp_create",
                                                initfuncs=['osp_initialize'],
                                                providerpaths=[AESCryptoProvider.example_provider],
                                                keystore=keystore)


class ROT13PythonCryptoProvider(PythonCryptoProvider):

    def __init__(self, keystore):
        super(ROT13PythonCryptoProvider,self).__init__()
        self.keystore = keystore

    def load_key(self, type, keyid):
        return self.keystore.get_key(keyid)

    def generate_iv(self):
        return "wibble"

    def sign(self, inputs):
        return "gronk"

    def verify_signature(self, inputs, signature):
        return signature == b"gronk"

    def encrypt_real(self, input, iv):
        if codecs.decode(input,'utf-8').endswith('\n'):
            raise couchbase_v2.exceptions.InternalError("passing back string containing newline")
        logging.debug("encrypting with input={} iv={}".format(repr(input),repr(iv)))
        encoded = codecs.encode(codecs.decode(input,'utf-8'), 'rot_13')
        return encoded

    def decrypt_real(self, input, iv):
        encoded = codecs.encode(codecs.decode(input,'utf-8'), 'rot_13')
        return encoded

    if _LCB.PYCBC_CRYPTO_VERSION<1:
        def encrypt(self, input, key, iv):
            return self.encrypt_real(input, iv)

        def decrypt(self, input, key, iv):
            return self.decrypt_real(input, iv)

    else:
        def encrypt(self, input, iv):
            return self.encrypt_real(input, iv)

        def decrypt(self, input, iv):
            return self.decrypt_real(input, iv)

    def get_key_id(self):
        return 'key'


class FieldEncryptionTests(ConnectionTestCase):
    def setUp(self, **kwargs):
        super(FieldEncryptionTests,self).setUp(**kwargs)

    def test_keystore_returns_correct_value(self):
        keystore = InMemoryKeyStore()
        keystore.set_key('key', 'my-secret')
        key = keystore.get_key('key')
        self.assertEqual('my-secret', key)

    def test_aes_c_encryption(self):
        raise SkipTest("C Crypto module not found, skipping")
        # create key store & encryption provider
        keystore = InMemoryKeyStore()
        keystore.set_key('key', 'my-secret')
        try:
            provider = AESCryptoProvider(keystore=keystore)
        except couchbase_v2.exceptions.NotFoundError:
            raise SkipTest("C Crypto module not found, skipping")

        document = {'sensitive': 'secret'}
        # register encryption provider with LCB
        self.cb.register_crypto_provider('aes256', provider)

        # encrypt document

        document = self.cb.encrypt_fields(document, [{'alg': 'aes256', 'name': 'sensitive', 'kid': 'key'}], "crypto_")
        self.assertEqual(document, {'crypto_sensitive': {'alg': 'aes256', 'ciphertext': 'LYOFcKPUcQiFhbyYVShvrg==',
                                                         'iv': 'ZedmvjWy0lIrLn6OmQmNqQ==', 'kid': 'key',
                                                         'sig': 'zUJOrVxGlyNXrOhAM+PAvDQ3frXFpvEyHZuQLw9ym9U='}})
        # write document to cluster
        key = self.gen_key('crypto-test')
        self.cb.upsert(key, document)

        # # read document (unencrypted)
        rv = self.cb.get(key)
        decrypted_document = self.cb.decrypt_fields(rv.value, "crypto_")
        # verify encrypted field can be read
        self.assertEqual(decrypted_document, {'sensitive': 'secret'})

        # remove encryption provider
        self.cb.unregister_crypto_provider('aes256')

        # read document (encrypted)
        rv = self.cb.get(key)
        if "sensitive" in rv.value.keys():
            self.assertNotEqual(rv.value["sensitive"], "secret")
        elif "crypto_sensitive" in rv.value.keys():
            self.assertNotEqual(rv.value["crypto_sensitive"], "secret")

    def test_pure_python_encryption(self):
        # create key store & encryption provider
        document, fieldspec, provider = self._setup_encryption()
        # encrypt document
        document = self.cb.encrypt_fields(document, [fieldspec], "crypto_")
        expected = {'ciphertext': 'ImZycGVyZyI=', 'iv': 'd2liYmxl', 'sig': 'Z3Jvbms=', 'kid': 'key'}
        orig_fields = {'alg': 'rot13'}
        expected_orig = {k:orig_fields[k] for k in orig_fields if k in fieldspec}
        expected.update(expected_orig)

        self.assertEqual(document, {'crypto_sensitive': expected})
        # write document to cluster
        key = self.gen_key('crypto-test')
        self.cb.upsert(key, document)

        # # read document (unencrypted)
        rv = self.cb.get(key)
        decrypt_args = self.get_decrypt_args(fieldspec)
        decrypted_document = self.cb.decrypt_fields(rv.value, *decrypt_args)
        # verify encrypted field can be read
        self.assertEqual(decrypted_document, {'sensitive': 'secret'})

        # remove encryption provider
        self.cb.unregister_crypto_provider('rot13')

        # read document (encrypted)
        rv = self.cb.get(key)
        if "sensitive" in rv.value.keys():
            self.assertNotEqual(rv.value["sensitive"], "secret")
        elif "crypto_sensitive" in rv.value.keys():
            self.assertNotEqual(rv.value["crypto_sensitive"], "secret")

        self.cb.register_crypto_provider('rot13', provider)

        emptystring = {'sensitive': ''}
        document = self.cb.encrypt_fields(emptystring, [fieldspec], "crypto_")
        self.assertEqual(self.cb.decrypt_fields(document, *decrypt_args), emptystring)

        newlineonly = {'sensitive': '\n'}
        document = self.cb.encrypt_fields(newlineonly, [fieldspec], "crypto_")
        self.assertEqual(self.cb.decrypt_fields(document, *decrypt_args), newlineonly)

    def get_decrypt_args(self, fieldspec):
        decrypt_args = []
        if _LCB.PYCBC_CRYPTO_VERSION > 0:
            decrypt_args.append([fieldspec])
        decrypt_args.append("crypto_")
        return decrypt_args

    def _setup_encryption(self):
        fieldspec = {'alg': 'rot13', 'name': 'sensitive'}
        if _LCB.PYCBC_CRYPTO_VERSION < 1:
            fieldspec['kid'] = 'key'
        keystore = InMemoryKeyStore()
        keystore.set_key('key', 'my-secret')
        provider = ROT13PythonCryptoProvider(keystore)
        document = {'sensitive': 'secret'}
        # register encryption provider with LCB
        try:
            self.cb.register_crypto_provider('rot13', provider)
        except couchbase_v2.exceptions.NotSupportedError as e:
            raise SkipTest(e)
        return document, fieldspec, provider

    def test_encryption_exceptions(self):
        # encrypt document
        for name, rcs in  _LCB.CRYPTO_EXCEPTIONS.items():
            exceptions = list(type(couchbase_v2.exceptions.exc_from_rc(rc)) for rc in rcs)
            document, fieldspec, provider = self._setup_encryption()
            def dummy(*args,**kwargs):
                raise couchbase_v2.exceptions.TemporaryFailError(params=dict(rc=_LCB.LCB_ETMPFAIL))
            logging.error("corrupting method:{}".format(name))
            setattr(provider, name, dummy)
            valid_exception_raised = False
            actual_e = None
            try:
                document = self.cb.encrypt_fields(document, [fieldspec], "crypto_")
                decrypt_args = self.get_decrypt_args(fieldspec)
                self.cb.decrypt_fields(document, *decrypt_args)
            except tuple(exceptions) as e:
                actual_e = e
                logging.error("Caught valid exception {}".format(e))
                self.assertEqual('rot13', e.objextra['alias'])
                valid_exception_raised = True
            self.assertTrue(valid_exception_raised, msg = "None of {} detected in [{}]".format(str(exceptions),repr(actual_e)))

    def test_keysize_exception(self):
        try:
            raise CryptoProviderKeySizeException(params={"objextra":{"alias":"fish"}})
        except CryptoProviderKeySizeException as e:
            self.assertRegex(e.message,r'.*alias: fish.*')
        try:
            raise CryptoProviderKeySizeException(params={"objextra":{"alias":"fish","expected_keysize":"1","configured_keysize":"900000000000"}})
        except CryptoProviderKeySizeException as e:
            self.assertRegex(e.message,r'.*alias: fish.*Expected key size was 1.*configured key size is 900000000000.*')