==============
Subdocument
==============

.. contents::
    :local:

.. module:: couchbase.subdocument

Enumerations
======================

.. autoenum:: StoreSemantics

Lookup Operations
======================

.. autofunction:: count
.. autofunction:: exists
.. autofunction:: get

Mutation Operations
======================

.. autofunction:: array_addunique
.. autofunction:: array_append
.. autofunction:: array_insert
.. autofunction:: array_prepend
.. autofunction:: counter
.. autofunction:: decrement
.. autofunction:: increment
.. autofunction:: remove
.. autofunction:: replace
.. autofunction:: upsert

Options
======================

.. module:: couchbase.options
    :noindex:

.. autoclass:: LookupInOptions
    :noindex:

.. autoclass:: MutateInOptions
    :noindex:

Results
======================

.. module:: couchbase.result
    :noindex:

.. class:: LookupInResult
    :noindex:

    .. autoproperty:: cas
        :noindex:
    .. autoproperty:: content_as
        :noindex:

.. class:: MutateInResult
    :noindex:

    .. autoproperty:: cas
        :noindex:
