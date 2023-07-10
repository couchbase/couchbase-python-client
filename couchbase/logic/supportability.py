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

import warnings
from typing import Optional


class CouchbaseDeprecationWarning(UserWarning):
    """
    Couchbase Python SDK Warning Category
    """


class Supportability:
    @classmethod
    def import_deprecated(cls, old_path, new_path):
        def decorator(cls):
            old_init = cls.__init__

            def new_init(self, *args, **kwargs):
                msg = (f"Importing {cls.__name__} from {old_path} is deprecated "
                       "and will be removed in a future release. "
                       f" Import {cls.__name__} from {new_path} instead.")
                warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=2)
                old_init(self, *args, **kwargs)

            cls.__init__ = new_init
            return cls
        return decorator

    @classmethod
    def class_deprecated(cls, use_instead):
        def decorator(cls):
            old_init = cls.__init__

            def new_init(self, *args, **kwargs):
                msg = (f"Class {cls.__name__} is deprecated "
                       "and will be removed in a future release. "
                       f"Use {use_instead} instead.")
                warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=2)
                old_init(self, *args, **kwargs)

            cls.__init__ = new_init
            return cls
        return decorator

    @staticmethod
    def class_property_deprecated(property,  # type: str
                                  use_instead  # type: str
                                  ) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided class property is deprecated.

        Args:
            property (str): The name of the deprecated property
            use_instead (str): The name of the property to use instead of the deprecated property.
        """
        message = (f"Class property {property} is deprecated and will be removed in a future release. "
                   f"Use {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=2)

    @staticmethod
    def method_param_deprecated(param,  # type: str
                                use_instead  # type: str
                                ) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided param is deprecated.

        Args:
            param (str): The name of the deprecated param
            use_instead (str): The name of the param to use instead of the deprecated param.
        """
        message = (f"Method parameter {param} is deprecated and will be removed in a future release. "
                   f"Use {use_instead} instead.")
        warnings.warn(message, CouchbaseDeprecationWarning, stacklevel=2)

    @staticmethod
    def option_deprecated(param,  # type: str
                          use_instead=None,  # type: Optional[str]
                          message=None,  # type: Optional[str]
                          ) -> None:
        """Issue a `CouchbaseDeprecationWarning` indicating the provided param is deprecated.

        Args:
            param (str): The name of the deprecated param
            use_instead (Optional, str): The name of the param to use instead of the deprecated param.
            message (Optional, str): A message to have in the warning to add context.
        """
        msg = f"Option {param} is deprecated and will be removed in a future release. "
        if use_instead:
            msg += f"Use {use_instead} instead. "
        if message:
            msg += message

        warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=2)


class RemoveProperty:
    """Used to override a get descriptor for a class property and raise an AttributeError to prevent access.

    This helper class should only be used in **rare** instances.  Specifically, it allows for an inheritance
    structure to remain intact while removing a property from a subclass.  In an ideal scenario, the hierarchy
    structure is revisted, but in some cases it is easier to keep the current structure.  This class provides a
    work-around.
    """

    def __init__(self, prop):
        self._prop = prop

    def __get__(self, instance, cls):
        raise AttributeError(f'Property "{self._prop}" is no longer a part of the {cls.__name__} class.')
