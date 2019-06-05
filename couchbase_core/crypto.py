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
import platform

from couchbase_core._libcouchbase import CryptoProvider
import ctypes.util
import ctypes
import logging
import os.path
import abc
import couchbase_core.exceptions as exceptions
import couchbase_core._libcouchbase as _LCB


class InMemoryKeyStore(object):

    def __init__(self):
        self.keys = {}

    def get_key(self, keyName):
        return self.keys.get(keyName) or None

    def set_key(self, keyName, key):
        self.keys[keyName] = key


class PythonCryptoProvider_V0(CryptoProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load_key(self, type, keyid):
        """
        Load a decryption/encryption key, as selected by the type
        :param type: LCBCRYPTO_KEY_ENCRYPT or LCBCRYPTO_KEY_DECRYPT
        :param keyid: Key ID to retrieve
        """
        pass

    @abc.abstractmethod
    def generate_iv(self):
        """
        Return an IV for use with decryption/encryption.
        """
        pass

    @abc.abstractmethod
    def sign(self, inputs):
        """
        Sign the inputs provided.
        :param inputs: List of strings to sign against
        :param signature: Signature
        """
        pass

    @abc.abstractmethod
    def verify_signature(self, inputs, signature):
        """
        Verify the inputs provided against the signature given.
        :param inputs: List of strings to verify signature against
        :param signature: Signature
        """
        pass

    @abc.abstractmethod
    def encrypt(self, input, key, iv):
        """
        Encrypt the input string using the key and iv
        :param input: input string
        :param key: actual encryption key
        :param iv: iv for encryption
        """
        pass

    @abc.abstractmethod
    def decrypt(self, input, key, iv) :
        """
        Encrypt the input string using the key and iv
        :param input: input string
        :param key: actual decryption key
        :param iv: iv for decryption
        """
        pass


class PythonCryptoProvider_V1(CryptoProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def generate_iv(self):
        """
        Return an IV for use with decryption/encryption.
        """
        pass

    @abc.abstractmethod
    def sign(self, inputs):
        """
        Sign the inputs provided.
        :param inputs: List of strings to sign against
        :param signature: Signature
        """
        pass

    @abc.abstractmethod
    def verify_signature(self, inputs, signature):
        """
        Verify the inputs provided against the signature given.
        :param inputs: List of strings to verify signature against
        :param signature: Signature
        """
        pass


    @abc.abstractmethod
    def encrypt(self, input, iv):
        """
        Encrypt the input string using the iv
        :param input: input string
        :param iv: iv for encryption
        """
        pass

    @abc.abstractmethod
    def decrypt(self, input, iv) :
        """
        Decrypt the input string using the iv
        :param input: input string
        :param iv: iv for decryption
        """
        pass

    @abc.abstractmethod
    def get_key_id(self):
        """
        Return the key ID
        :return: the key id
        """

if _LCB.PYCBC_CRYPTO_VERSION == 0:
    PythonCryptoProvider=PythonCryptoProvider_V0
else:
    PythonCryptoProvider=PythonCryptoProvider_V1


class CTypesCryptoProvider(CryptoProvider):
    postfix = {"Linux": "so", "Darwin": "dylib", "Windows": "dll"}[platform.system()]

    def key_loader(self, direction, key_id):
        key = self.keystore.get_key(key_id)
        logging.error("loading key from {}: {}".format(key_id, key))
        return key

    def __init__(self, providername, providerfunc, initfuncs=[], providerpaths=[], keystore=None, **kwargs):
        """
        Initialise CTypesCryptoProvider
        :param providername: library name as used by ctypes.util.find_library
        :param providerfunc: name of function which returns C Provider
        :param initfuncs: list of any initialisation functions required, in order
        :param providerpaths: list of functions that take providername as argument and return a path to try
            loading the library from
        :param keystore: optional keystore
        """
        self.keystore = keystore
        providerpaths.append(ctypes.util.find_library)
        crypto_dll = None
        for library in providerpaths:
            logging.debug("Trying " + str(library))
            libname = os.path.abspath(library(providername))
            logging.debug("Got (providername)=" + libname)
            try:
                crypto_dll = ctypes.cdll.LoadLibrary(libname)
                break
            except Exception as e:
                logging.error(str(e))
                raise exceptions.NotFoundError("Couldn't load provider shared library")
        if crypto_dll:
            logging.debug("Crypto provider DLL=[" + repr(crypto_dll))
            try:
                for func in initfuncs:
                    getattr(crypto_dll, func)()
                provfunc = getattr(crypto_dll, providerfunc)
                logging.debug("Crypto provider Func=" + str(provfunc))
                provfunc.restype = ctypes.c_void_p
                provider = provfunc()
                logging.debug("Crypto provider=" + format(provider, '02x'))
            except Exception as e:
                logging.debug("Problem executing Crypto provider: " + str(e))
                raise exceptions.NotFoundError("Couldn't initialise crypto provider shared library")

            super(CTypesCryptoProvider, self).__init__(provider=provider)
            if self.keystore:
                setattr(self, 'load_key', self.key_loader)
        else:
            raise exceptions.NotFoundError("Couldn't find provider shared library")
