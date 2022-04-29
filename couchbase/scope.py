from couchbase.logic.scope import ScopeLogic

"""
** DEPRECATION NOTICE **

Once the deprecated Scope import from couchbase.collection is removed, this class
can be replaced w/ the ScopeLogic class and the ScopeLogic class removed.  The
hierarchy was created to help w/ 3.x imports.

"""


class Scope(ScopeLogic):
    """Create a Couchbase Scope instance.

    Exposes the operations which are available to be performed against a scope. Namely the ability to access
    to Collections for performing operations.

    Args:
        bucket (:class:`~.Bucket`): A :class:`~.Bucket` instance.
        scope_name (str): Name of the scope.

    """
