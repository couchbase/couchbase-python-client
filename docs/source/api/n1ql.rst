############
N1QL Queries
############

.. currentmodule:: couchbase.n1ql

.. class:: N1QLQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. autoattribute:: consistency
    .. autoattribute:: encoded
    .. autoattribute:: adhoc
    .. autoattribute:: timeout

.. autodata:: CONSISTENCY_NONE
.. autodata:: CONSISTENCY_REQUEST


.. class:: N1QLRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. automethod:: get_single_result
