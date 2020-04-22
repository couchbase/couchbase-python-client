=========================
Administrative Operations
=========================

.. module:: couchbase.management.admin

The :class:`~.Admin` provides several convenience methods
to perform common API requests.

.. warning:: The interface here is provided as a convenience only
    and its interface may change.


To create an administrative handle, simply instantiate a new
:class:`Admin` object. Note that unlike the :class:`~couchbase_core.client.Client`,
the :class:`Admin` constructor does not accept a connection string. This is
deliberate, as the administrative API communicates with a single node, on
a well defined port (whereas the :class:`~couchbase_core.client.Client` object communicates with
one or more nodes using a variety of different protocols).


.. autoclass:: Admin
    :members:
