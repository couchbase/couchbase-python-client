#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

__all__ = [
    'AuthDomain',
    'Authenticator',
    'CertificateAuthenticator',
    'JwtAuthenticator',
    'PasswordAuthenticator',
]

from enum import IntEnum
from typing import Any

from couchbase.common._utils import fields_repr, redacted
from couchbase.common.exceptions import InvalidArgumentException


class Authenticator(dict):
    """The credentials a cluster connects with.

    Each subclass is one authentication mechanism. An instance is a dict of the settings the connection takes, and its
    repr hides the password, the token and the key path.
    """

    _REDACTED_KEYS = frozenset({'password', 'jwt_token', 'key_path'})

    def __repr__(self) -> str:
        return fields_repr(self, **{k: redacted(v) if k in self._REDACTED_KEYS else v for k, v in self.items()})


class PasswordAuthenticator(Authenticator):
    """Password authentication mechanism.

    Args:
        username (str): Username to use for authentication.
        password (str): Password to use for authentication.
        cert_path (str, optional): Path of the certificate trust store. Defaults to None.
    """

    def __init__(self, username: str, password: str, cert_path: str | None = None, **kwargs: Any) -> None:
        """PasswordAuthenticator instance."""
        if not isinstance(username, str):
            msg = 'The username must be a str.'
            raise InvalidArgumentException(msg)

        if not isinstance(password, str):
            msg = 'The password must be a str.'
            raise InvalidArgumentException(msg)

        if cert_path is not None and not isinstance(cert_path, str):
            msg = 'The cert_path must be a str representing the path to the certificate trust store.'
            raise InvalidArgumentException(msg)

        allowed_sasl_mechanisms = kwargs.pop('allowed_sasl_mechanisms', None)
        if allowed_sasl_mechanisms is not None:
            msg = None
            if isinstance(allowed_sasl_mechanisms, str):
                allowed_sasl_mechanisms = allowed_sasl_mechanisms.split(',')
            if isinstance(allowed_sasl_mechanisms, list):
                if not all(map(lambda x: isinstance(x, str), allowed_sasl_mechanisms)):
                    msg = 'The allowed_sasl_mechanisms must be a list of str SASL mechanisms.'
            else:
                msg = ('The allowed_sasl_mechanisms must be a list of str SASL mechanisms '
                       ' or a comma separated str of SASL mechanisms.')
            if msg:
                raise InvalidArgumentException(msg)

        self._username = username
        self._password = password
        self._cert_path = cert_path
        self._allowed_sasl_mechanisms = allowed_sasl_mechanisms

        super().__init__(**self.as_dict())

    def valid_keys(self) -> list[str]:
        """**INTERNAL** The names of the cluster options this authenticator takes settings from."""
        return ['username', 'password', 'cert_path', 'sasl_mech_force', 'allowed_sasl_mechanisms']

    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The settings the connection takes."""
        d = {
            'username': self._username,
            'password': self._password,
            'allowed_sasl_mechanisms': self._allowed_sasl_mechanisms
        }
        if self._cert_path is not None:
            # couchbase++ wants this to be the trust_certificate
            d['trust_store_path'] = self._cert_path

        return d

    def __repr__(self) -> str:
        return fields_repr(self, username=self._username, password=redacted(self._password), cert_path=self._cert_path,
                           allowed_sasl_mechanisms=self._allowed_sasl_mechanisms)

    @staticmethod
    def ldap_compatible(username: str, password: str) -> PasswordAuthenticator:
        """A password authenticator that sends the password with SASL PLAIN, which LDAP users need.

        PLAIN sends the password as it is, so use it over TLS.

        Args:
            username (str): The name of the user.
            password (str): The user's password.

        Returns:
            :class:`.PasswordAuthenticator`: An authenticator allowing only the PLAIN mechanism.
        """
        auth = PasswordAuthenticator(username, password, allowed_sasl_mechanisms=['PLAIN'])
        return auth


class JwtAuthenticator(Authenticator):
    """JWT (JSON Web Token) authentication mechanism.

    Uses OAUTHBEARER SASL mechanism for KV connections and Bearer token
    for HTTP services.

    Args:
        token (str): The JWT token to use for authentication.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If token is not a string.
    """

    def __init__(self, token: str) -> None:
        """JwtAuthenticator instance."""
        if not isinstance(token, str):
            msg = 'The token must be a str.'
            raise InvalidArgumentException(msg)

        self._token = token

        super().__init__(**self.as_dict())

    def valid_keys(self) -> list[str]:
        """**INTERNAL** The names of the cluster options this authenticator takes settings from."""
        return ['jwt_token']

    def __repr__(self) -> str:
        return fields_repr(self, token=redacted(self._token))

    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The settings the connection takes."""
        return {
            'jwt_token': self._token
        }


class CertificateAuthenticator(Authenticator):
    """Certificate authentication mechanism.

    Args:
        cert_path (str): Path to the client certificate. Defaults to None.
        key_path (str): Path to the client key. Defaults to None.
        trust_store_path (str, optional): Path of the certificate trust store. Defaults to None.
    """

    def __init__(self,
                 cert_path: str | None = None,
                 key_path: str | None = None,
                 trust_store_path: str | None = None
                 ) -> None:
        """CertificateAuthenticator instance."""
        if not isinstance(cert_path, str):
            msg = 'The cert_path must be a str representing the path to the client certificate.'
            raise InvalidArgumentException(msg)

        if not isinstance(key_path, str):
            msg = 'The key_path must be a str representing the path to the client key.'
            raise InvalidArgumentException(msg)

        if trust_store_path is not None and not isinstance(trust_store_path, str):
            msg = 'The trust_store_path must be a str representing the path to the certificate trust store.'
            raise InvalidArgumentException(msg)

        self._trust_store_path = trust_store_path
        self._cert_path = cert_path
        self._key_path = key_path

        super().__init__(**self.as_dict())

    def valid_keys(self) -> list[str]:
        """**INTERNAL** The names of the cluster options this authenticator takes settings from."""
        return ['cert_path', 'key_path', 'trust_store_path']

    def __repr__(self) -> str:
        return fields_repr(self, cert_path=self._cert_path, key_path=redacted(self._key_path),
                           trust_store_path=self._trust_store_path)

    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The settings the connection takes."""
        d = {
            'cert_path': self._cert_path,
            'key_path': self._key_path
        }
        if self._trust_store_path is not None:
            d['trust_store_path'] = self._trust_store_path

        return d


class AuthDomain(IntEnum):
    """Where a user is managed.

    Local users are managed by Couchbase Server, and External users by an outside service such as LDAP.
    """
    Local = 0
    External = 1

    @classmethod
    def to_str(cls, value: AuthDomain) -> str:
        """**INTERNAL** The domain as Couchbase Server names it."""
        if value == cls.External:
            return "external"
        else:
            return "local"

    @classmethod
    def from_str(cls, value: str | None) -> AuthDomain:
        """**INTERNAL** The domain Couchbase Server named, Local for any name but ``external``."""
        if value == "external":
            return cls.External
        else:
            return cls.Local
