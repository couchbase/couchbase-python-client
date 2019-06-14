############
N1QL Queries
############

.. currentmodule:: couchbase_core.n1ql

.. class:: N1QLQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. automethod:: consistent_with
    .. autoattribute:: consistency
    .. autoattribute:: encoded
    .. autoattribute:: adhoc
    .. autoattribute:: timeout
    .. autoattribute:: cross_bucket

.. autodata:: NOT_BOUNDED
.. autodata:: REQUEST_PLUS
.. autodata:: UNBOUNDED

.. currentmodule:: couchbase_core.mutation_state
.. autoclass:: MutationState

.. currentmodule:: couchbase_core.n1ql
.. class:: N1QLRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. autoattribute:: meta
    .. automethod:: get_single_result
