#  Copyright 2016-2026. Couchbase, Inc.
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

"""The registered Configuration Profiles, and the names they are registered under.

A profile is a named bundle of cluster options a caller applies in one call, so the registry
and the names have to be reachable without reaching the options classes that consume them.
"""

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

__all__ = [
    'CONFIG_PROFILES',
    'ConfigProfile',
    'ConfigProfiles',
    'KnownConfigProfiles',
    'WanDevelopmentProfile',
]

from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from couchbase.common.options import ClusterOptions


class KnownConfigProfiles(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    Represents the name of a specific configuration profile that is associated with predetermined cluster options.

    """
    WanDevelopment = 'wan_development'

    @classmethod
    def from_str(cls, value: str) -> KnownConfigProfiles:
        if isinstance(value, str):
            if value == cls.WanDevelopment.value:
                return cls.WanDevelopment

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Expected str representation of type KnownConfigProfiles."))

    @classmethod
    def to_str(cls, value: KnownConfigProfiles | str) -> str:
        if isinstance(value, KnownConfigProfiles):
            return value.value

        # just retun the str to allow for future customer config profiles
        if isinstance(value, str):
            return value

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Expected config profile to be either of type "
                                                "KnownConfigProfiles or str representation "
                                                "of KnownConfigProfiles."))


class ConfigProfile(ABC):
    """
    **VOLATILE** This API is subject to change at any time.

    This is an abstract base class intended to use with creating Configuration Profiles.  Any derived class
    will need to implement the :meth:`apply` method.
    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def apply(self, options: ClusterOptions) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided options to ClusterOptions. This method will need to be implemented in derived classes.

        Args:
            options (:class:`~couchbase.options.ClusterOptions`): The options the profile will apply toward.
        """
        pass


class WanDevelopmentProfile(ConfigProfile):
    """
    **VOLATILE** This API is subject to change at any time.

    The WAN Development profile sets various timeout options that are useful when develoption in a WAN environment.
    """

    def __init__(self) -> None:
        super().__init__()

    def apply(self, options: ClusterOptions) -> None:
        # Every key set here has to be one ClusterTimeoutOptions accepts, which is
        # cluster_keys('ClusterTimeoutOptions') in couchbase/common/_option_args.py.
        options['kv_timeout'] = timedelta(seconds=20)
        options['kv_durable_timeout'] = timedelta(seconds=20)
        options['connect_timeout'] = timedelta(seconds=20)
        options['analytics_timeout'] = timedelta(seconds=120)
        options['query_timeout'] = timedelta(seconds=120)
        options['search_timeout'] = timedelta(seconds=120)
        options['management_timeout'] = timedelta(seconds=120)
        options['views_timeout'] = timedelta(seconds=120)
        options['dns_srv_timeout'] = timedelta(seconds=20)  # time to make DNS-SRV query
        options['resolve_timeout'] = timedelta(seconds=20)  # time to resolve hostnames
        options['bootstrap_timeout'] = timedelta(seconds=120)  # overall bootstrap timeout


class ConfigProfiles():
    """
    **VOLATILE** This API is subject to change at any time.

    The `ConfigProfiles` class is responsible for keeping track of registered/known Configuration
    Profiles.
    """

    def __init__(self) -> None:
        self._profiles: dict[str, ConfigProfile] = {}
        self.register_profile(KnownConfigProfiles.WanDevelopment.value, WanDevelopmentProfile())

    def apply_profile(self, profile_name: str, options: ClusterOptions) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name (str):  The name of the profile to apply.
            options (:class:`~couchbase.options.ClusterOptions`): The options to apply the ConfigProfile options
                toward. The ConfigProfile options will override any matching option(s) previously set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.
        """
        if profile_name not in self._profiles:
            raise InvalidArgumentException(f'{profile_name} is not a registered profile.')

        self._profiles[profile_name].apply(options)

    def register_profile(self, profile_name: str, profile: ConfigProfile) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Register a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to register.
            profile (:class:`~couchbase.options.ConfigProfile`): The :class:`~couchbase.options.ConfigProfile`
                to register.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not derived
            from :class:`~couchbase.options.ConfigProfile`.

        """
        if not issubclass(profile.__class__, ConfigProfile):
            raise InvalidArgumentException('A Configuration Profile must be derived from ConfigProfile')

        self._profiles[profile_name] = profile

    def unregister_profile(self, profile_name: str) -> ConfigProfile | None:
        """
        **VOLATILE** This API is subject to change at any time.

        Unregister a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to unregister.

        Returns
            Optional(:class:`~couchbase.options.ConfigProfile`): The unregistered
            :class:`~couchbase.options.ConfigProfile`
        """

        return self._profiles.pop(profile_name, None)


"""
**VOLATILE** The ConfigProfiles API is subject to change at any time.
"""
CONFIG_PROFILES = ConfigProfiles()
