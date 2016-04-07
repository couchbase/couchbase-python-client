.. module:: couchbase.subdocument

================
Sub-Document API
================

.. currentmodule:: couchbase.subdocument

The functions in this module can be used to specify operations to the
:cb_bmeth:`lookup_in` and :cb_bmeth:`mutate_in` methods. Both the
`mutate_in` and `lookup_in` methods can take multiple operations.

Any given operation is either valid in :cb_bmeth:`lookup_in` or
:cb_bmeth:`mutate_in`; never both.


Internally every function in this module returns an object
specifying the path, options, and value of the command, so for example:

.. code-block:: python

    cb.mutate_in(key,
                 SD.upsert('path1', 'value1'),
                 SD.insert('path2', 'value2', create_parents=True))

really becomes

.. code-block:: python

    cb.mutate_in(key,
                 (CMD_SUBDOC_UPSERT, 'path1', 'value1', 0),
                 (CMD_SUBDOC_INSERT, 'path2', 'value2', 1))


Thus, the actual operations are performed when the `mutate_in` or `lookup_in`
methods are executed, the functions in this module just acting as an interface
to specify what sorts of operations are to be executed.

Throughout the SDK documentation, this module is referred to as ``SD`` which
is significantly easier to type than ``couchbase.subdocument``. This is done
via

.. code-block:: python

    import couchbase.subdocument as SD

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


