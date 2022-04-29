from __future__ import annotations

import warnings


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
