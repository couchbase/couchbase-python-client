=====================
Logging and Telemetry
=====================

.. contents::
    :local:

Logging
=======

The SDK logs from two places.  The native core, which talks to the cluster, has a log of its own.  The
Python side of the SDK logs through Python's :mod:`logging`, under loggers named ``couchbase``:

* ``couchbase.threshold``, for `Threshold logging`_;
* ``couchbase.metrics``, for `Metrics logging`_;
* ``couchbase.transactions``, for transactions.

The native core logs nothing until it is asked to, in one of two ways, and an application uses one or
the other.

From the environment
--------------------

:envvar:`PYCBC_LOG_LEVEL` turns on the native core's log, and sets the level of the ``couchbase``
logger to match.  It needs no change to the code, which makes it the way to investigate a problem in an
application already deployed.  :doc:`environment_variables` describes it and the variables that go
with it.

From the code
-------------

:func:`couchbase.configure_logging` sends the native core's log to a Python logger instead, so its
records reach the handlers the application already has.  It takes the logger's name and the level to
log at:

.. code-block:: python

    import logging

    from couchbase import configure_logging

    logging.basicConfig(level=logging.INFO)

    # The native core's records, through the logger 'my_app.couchbase'.
    configure_logging('couchbase', level=logging.DEBUG, parent_logger=logging.getLogger('my_app'))

    # The SDK's own Python loggers are configured like any other.
    logging.getLogger('couchbase').setLevel(logging.INFO)

It can be called before or after the first cluster connects, and raises ``RuntimeError`` when
:envvar:`PYCBC_LOG_LEVEL` is set.

Redaction
---------

A log can hold document keys, query statements, bucket names and hostnames, which an application may
not be free to share.  ``log_redaction=True`` in :class:`~couchbase.options.ClusterOptions`, or
``log_redaction=true`` in the connection string, makes the SDK tag each such value where it writes it,
in the native core's log and in the ``couchbase`` loggers alike:

* ``<ud>...</ud>``, user data: document keys and values, query statements, usernames;
* ``<md>...</md>``, metadata: bucket, scope, collection and index names;
* ``<sd>...</sd>``, system data: hostnames, IP addresses and ports.

The threshold logging report, for example, is then logged as a whole as system data:

.. code-block:: text

    <sd>{"kv":{"total_count":3,"top_requests":[{"operation_name":"upsert","total_duration_us":1500}]}}</sd>

The SDK only tags; it removes nothing, so the log stays as useful for the application's own
debugging.  Redacting happens afterwards, on a copy of the log, with ``cblogredaction``, the tool that
ships with Couchbase Server.  It replaces each piece of tagged user data with a salted hash, so a value
logged twice still matches itself, and the copy can be shared.

Redaction is for the whole process: once any cluster connects with it on, it stays on, for every
cluster, until the process exits.

Threshold logging
=================

Unless told otherwise, the SDK times every operation, and every 10 seconds logs a report of the slowest
operations that took longer than a threshold for their service: 500 milliseconds for key-value
operations, and a second for the other services.  The report is one line of JSON at
``INFO``, to the ``couchbase.threshold`` logger, with up to 10 operations per service:

.. code-block:: json

    {"kv":{"total_count":3,"top_requests":[{"operation_name":"upsert","total_duration_us":1500,
    "last_dispatch_duration_us":1200}]}}

The thresholds, the number of operations kept and the interval are set by
:class:`~couchbase.options.ClusterTracingOptions`:

.. code-block:: python

    from datetime import timedelta

    from couchbase.options import ClusterOptions, ClusterTracingOptions

    tracing = ClusterTracingOptions(tracing_threshold_kv=timedelta(milliseconds=100),
                                    tracing_threshold_queue_flush_interval=timedelta(seconds=30))
    options = ClusterOptions(auth, tracing_options=tracing)

``enable_tracing=False`` turns threshold logging off, and an application's own tracer, given as
``tracer`` in :class:`~couchbase.options.ClusterOptions`, replaces it.

Metrics logging
===============

Unless told otherwise, the SDK records the latency of every operation, and every 10 minutes logs the
percentiles for each service and operation.  The report is one line of JSON at ``INFO``, to the
``couchbase.metrics`` logger:

.. code-block:: json

    {"meta":{"emit_interval_s":600.0},"operations":{"kv":{"upsert":{"total_count":100,
    "percentiles_us":{"50.0":150,"90.0":350,"99.0":800,"99.9":1200,"100.0":1500}}}}}

:class:`~couchbase.options.ClusterMetricsOptions` sets the interval, as ``emit_interval``, and
``enable_metrics=False`` turns it off.  An application's own meter, given as ``meter`` in
:class:`~couchbase.options.ClusterOptions`, replaces it.

Both reports are logged at ``INFO``, and Python's default logging shows only ``WARNING`` and above, so
an application that has not configured logging sees neither.

Orphaned responses
==================

A response that arrives after its operation has timed out or been cancelled is an orphan.  The native
core collects them, and every 10 seconds logs a sample at ``WARNING``, in its own log.
:class:`~couchbase.options.ClusterOrphanReportingOptions` sets the sample size and the interval, and
``enable_orphan_reporting=False`` turns it off.

Turning it off
==============

Threshold logging and metrics logging run on the Python side of the SDK, and each costs every operation
a little.  An application that needs neither turns both off:

.. code-block:: python

    options = ClusterOptions(auth, enable_tracing=False, enable_metrics=False)

With both off, the SDK skips its tracing and metrics work for each operation entirely.  With only one
off, that report stops, but every operation still does the work for the other.  A ``tracer`` or a
``meter`` given in :class:`~couchbase.options.ClusterOptions` turns its half back on, whatever its flag
says.

Neither flag affects the report of orphaned responses, which the native core makes.
``enable_orphan_reporting=False`` turns that off.

OpenTelemetry
=============

The SDK can send its spans and its metrics to OpenTelemetry instead of logging them.  It needs the
``otel`` extra:

.. code-block:: console

    $ python3 -m pip install "couchbase[otel]"

:func:`~couchbase.observability.get_otel_tracer` and :func:`~couchbase.observability.get_otel_meter`
return a tracer and a meter for the cluster's options, from the global OpenTelemetry providers or from
the ones given:

.. code-block:: python

    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider

    from couchbase.observability import get_otel_meter, get_otel_tracer
    from couchbase.options import ClusterOptions

    tracer_provider = TracerProvider()
    meter_provider = MeterProvider()
    # Add the application's span processors and metric readers to the providers here.

    options = ClusterOptions(auth,
                             tracer=get_otel_tracer(tracer_provider),
                             meter=get_otel_meter(meter_provider))

A tracer given this way replaces threshold logging, and a meter replaces metrics logging.  To make an
operation's span the child of one of the application's own, pass that span as the operation's
``parent_span`` option.

:python_sdk_github:`The SDK's examples </tree/master/examples/observability>` export both to an OTLP
collector, with Jaeger and Prometheus behind it.

Another tracing or metrics library
==================================

A tracer is a :class:`~couchbase.observability.tracing.RequestTracer` that creates
:class:`~couchbase.observability.tracing.RequestSpan` objects, and a meter is a
:class:`~couchbase.observability.metrics.Meter` that creates
:class:`~couchbase.observability.metrics.ValueRecorder` objects.  To send the SDK's spans or metrics
to a library other than OpenTelemetry, implement those interfaces and pass the tracer or the meter in
:class:`~couchbase.options.ClusterOptions`, as above.
