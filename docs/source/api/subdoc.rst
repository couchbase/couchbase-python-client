================
Sub-Document API
================

.. currentmodule:: couchbase.subdocument

The functions in this module can be used to specify operations to the
:cb_bmeth:`lookup_in` and :cb_bmeth:`upsert_in` methods. Both the
`mutate_in` and `lookup_in` methods can take multiple operations.

Any given operation is either valid in :cb_bmeth:`lookup_in` or
:cb_bmeth:`mutate_in`; never both.

-----------------
Lookup Operations
-----------------

.. autofunction:: get
.. autofunction:: exists

-------------------
Mutation Operations
-------------------

.. autofunction:: upsert
.. autofunction:: replace
.. autofunction:: insert
.. autofunction:: array_append
.. autofunction:: array_prepend
.. autofunction:: array_insert
.. autofunction:: array_addunique
.. autofunction:: remove
.. autofunction:: counter


-------------
Result Object
-------------

.. autoclass:: couchbase.result.SubdocResult
    :members:

-----------
Path Syntax
-----------

The path syntax is hierarchical and follows that of N1QL. Use a dot (`.`)
to separate between components. A backtick may be used to escape dots or
other special characters. Considering the dictionary:

.. code-block:: python

    {
        'dict': {
            'nestedDict': {
                'value': 123
            },
            'nestedArray': [1,2,3],
            'literal.dot': 'Hello',
            'literal[]brackets': 'World'
        },
        'array': [1,2,3],
        'primitive': True
    }

Accessing paths can be done as:

- ``dict``
- ``dict.nestedDict``
- ``dict.nestedDict.value``
- ``dict.nestedArray``
- ``dict.nestedArray[0]``
- ``dict.nestedArray[-1]`` (gets last element)
- ``dict.`literal.dot```
- ``dict.`literal[]brackets```
- ``array``
- ``primitive``


