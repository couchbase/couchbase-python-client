############
N1QL Queries
############

.. warning::
    At the time of writing, N1QL is still an experimental feature. As such,
    both the server-side API as well as the client API are subject to change.


.. currentmodule:: couchbase.n1ql

.. class:: N1QLQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. autoattribute:: consistency
    .. autoattribute:: encoded
    .. autoattribute:: adhoc

.. autodata:: CONSISTENCY_NONE
.. autodata:: CONSISTENCY_REQUEST


.. class:: N1QLRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. automethod:: get_single_result