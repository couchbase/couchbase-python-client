from enum import IntEnum

from couchbase.exceptions import CouchbaseException
from typing import *


class MixedAuthException(CouchbaseException):
    """
    Cannot use old and new style auth together in the same cluster
    """
    pass


class NoBucketException(CouchbaseException):
    """
    Operation requires at least a single bucket to be open
    """

# TODO: refactor this into base class perhaps?
def _recursive_creds_merge(base, overlay):
    for k, v in overlay.items():
        base_k = base.get(k, None)
        if not base_k:
            base[k] = v
            continue
        if isinstance(v, dict):
            if isinstance(base_k, dict):
                base[k] = _recursive_creds_merge(base_k, v)
            else:
                raise Exception("Cannot merge dict and {}".format(v))
        else:
            raise Exception("Cannot merge non dicts")
    return base


class Authenticator(object):
    def __init__(self, cert_path=None):
        """
        :param cert_path: Path for SSL certificate (last in chain if multiple)
        """
        self._cert_path = cert_path

    def get_credentials(self, bucket=None):
        """
        Gets the credentials for a specified bucket. If bucket is
        `None`, gets the username and password for the entire cluster, if
        different.
        :param bucket: The bucket to act as context
        :return: A dictionary of (optional) scheme and credentials e.g. `{'scheme':'couchbases',options:{'username':'fred', 'password':'opensesame'}}`
        """
        return self.get_auto_credentials(bucket)

    @classmethod
    def unwanted_keys(cls):
        """
        The set of option keys that are definitely incompatible with this authentication style.
        """
        return set()

    @classmethod
    def unique_keys(cls):
        """
        The set of option keys, if any, that this authenticator uniquely possesses.
        """
        return set(cls.get_unique_creds_dict().keys())

    @classmethod
    def get_unique_creds_dict(cls):
        """
        User overridable
        A dictionary of authenticator-unique options and functions/lambdas of the form:
            function(self):
                return self.password
        e.g.
        {'certpath': lambda self: self.certpath}
        """
        return {}

    def _base_options(self, bucket, overlay):
        base_dict = {'options': {'certpath': self._cert_path} if self._cert_path else {}}
        return _recursive_creds_merge(base_dict, overlay)

    def get_cred_bucket(self, bucket, **overlay):
        """
        :param bucket:
        :return: returns the non-unique parts of the credentials for bucket authentication,
        as a dictionary of functions, e.g.:
        'options': {'username': self.username}, 'scheme': 'couchbases'}
        """
        return self._base_options(bucket, overlay)

    def get_cred_not_bucket(self, **overlay):
        """
        :param bucket:
        :return: returns the non-unique parts of the credentials for admin access
        as a dictionary of functions, e.g.:
        {'options':{'password': self.password}}
        """
        return self._base_options(None, overlay)

    def get_auto_credentials(self, bucket):
        """
        :param bucket:
        :return: returns a dictionary of credentials for bucket/admin
        authentication
        """

        result = {k: v(self) for k, v in self.get_unique_creds_dict().items()}
        if bucket:
            result.update(self.get_cred_bucket(bucket))
        else:
            result.update(self.get_cred_not_bucket())
        return result

    def supports_tls(self):
        return True

    def supports_non_tls(self):
        return True


class PasswordAuthenticator(Authenticator):
    def __init__(self,
                 username,          # type: str
                 password,          # type: str
                 cert_path=None     # type: str
                 ):
        """
        This class uses a single credential pair of username and password, and
        is designed to be used either with cluster management operations or
        with Couchbase 5.0 style usernames with role based access control.

        :param str username: username to use for auth.
        :param str password: password for the user.
        :param str cert_path: Path to the CA key.

        """
        super(PasswordAuthenticator, self).__init__(cert_path=cert_path)
        self.username = username
        self.password = password

    def get_cred_bucket(self, bucket, **overlay):
        return self.get_cred_not_bucket(**overlay)

    def get_cred_not_bucket(self, **overlay):
        merged = _recursive_creds_merge({'options': {'username': self.username, 'password': self.password}}, overlay)
        return super(PasswordAuthenticator, self).get_cred_not_bucket(**merged)

    @classmethod
    def unwanted_keys(cls):
        return {'password'}


class ClassicAuthenticator(Authenticator):
    def __init__(self, cluster_username=None,
                 cluster_password=None,
                 buckets=None,
                 cert_path=None):
        """
        Classic authentication mechanism.

        :param cluster_username:
            Global cluster username. Only required for management operations
        :type cluster_username: str
        :param cluster_password:
            Global cluster password. Only required for management operations
        :param buckets:
            A dictionary of `{bucket_name: bucket_password}`.
        :param cert_path:
            Path of the CA key
        """
        super(ClassicAuthenticator, self).__init__(cert_path=cert_path)
        self.username = cluster_username
        self.password = cluster_password
        self.buckets = buckets if buckets else {}

    def get_cred_not_bucket(self):
        return super(ClassicAuthenticator, self).get_cred_not_bucket(
            **{'options': {'username': self.username, 'password': self.password}})

    def get_cred_bucket(self, bucket, **overlay):
        merged = _recursive_creds_merge({'options': {'password': self.buckets.get(bucket)}}, overlay)
        return super(ClassicAuthenticator, self).get_cred_bucket(bucket, **merged)


class CertAuthenticator(Authenticator):

    def __init__(self,
                 cert_path=None,            # type: str
                 key_path=None,             # type: str
                 trust_store_path=None,     # type: str
                 cluster_username=None,     # type: str
                 cluster_password=None      # type: str
                 ):
        """
        Certificate authentication mechanism.

        :param str cluster_username: Global cluster username. Only required for management operations
        :param str cluster_password: Global cluster password. Only required for management operations
        :param str cert_path: Path to the CA key
        :param str key_path: Path to the key
        :param str trust_store_path: Path of the certificate trust store.
        """
        super(CertAuthenticator, self).__init__(cert_path=cert_path)

        self.username = cluster_username
        self.password = cluster_password
        self.keypath = key_path
        self.trust_store_path = trust_store_path

    @classmethod
    def get_unique_creds_dict(clazz):
        return {'keypath': lambda self: self.keypath,
                'truststorepath': lambda self: self.trust_store_path}

    def get_cred_bucket(self, bucket, **overlay):
        merged = _recursive_creds_merge(
            {'options': {'username': self.username}, 'scheme': 'couchbases'},
            overlay)
        return super(CertAuthenticator, self).get_cred_bucket(bucket, **merged)

    def get_cred_not_bucket(self):
        return super(CertAuthenticator, self).get_cred_not_bucket(**{'options': {'password': self.password}})

    def supports_non_tls(self):
        return False

    @classmethod
    def unwanted_keys(cls):
        return {'password'}

    def get_credentials(self, bucket=None):
        return self.get_auto_credentials(bucket)


class AuthDomain(IntEnum):
    """
    The Authentication domain for a user.
    Local: Users managed by Couchbase Server.
    External: Users managed by an external resource, eg LDAP.
    """
    Local = 0
    External = 1

    @classmethod
    def to_str(cls, value):
        if value == cls.External:
            return "external"
        else:
            return "local"

    @classmethod
    def from_str(cls, value):
        if value == "external":
            return cls.External
        else:
            return cls.Local
