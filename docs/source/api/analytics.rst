#################
Analytics Queries
#################

.. currentmodule:: couchbase_core.analytics

.. class:: AnalyticsQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. automethod:: consistent_with
    .. autoattribute:: encoded

.. class:: AnalyticsRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. autoattribute:: meta
    .. automethod:: get_single_result

.. class:: DeferredAnalyticsQuery

    .. automethod:: __init__
    .. automethod:: set_option
    .. autoattribute:: encoded

.. class:: DeferredAnalyticsRequest

    .. automethod:: __init__
    .. automethod:: __iter__
    .. automethod:: execute
    .. autoattribute:: meta
    .. automethod:: get_single_result

.. currentmodule:: couchbase_core.analytics_ingester

.. class:: AnalyticsIngester

    .. automethod:: __init__
    .. automethod:: __call__

