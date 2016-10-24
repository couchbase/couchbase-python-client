############
N1QL Queries
############

.. currentmodule:: couchbase.n1ql

.. class:: N1QLQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. automethod:: consistent_with
    .. autoattribute:: consistency
    .. autoattribute:: encoded
    .. autoattribute:: adhoc
    .. autoattribute:: timeout
    .. autoattribute:: cross_bucket

.. autodata:: UNBOUNDED
.. autodata:: REQUEST_PLUS

.. autoclass:: MutationState

.. class:: N1QLRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. autoattribute:: meta
    .. automethod:: get_single_result
