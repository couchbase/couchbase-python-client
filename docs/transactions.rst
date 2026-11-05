============
Transactions
============

A transaction reads and writes documents, in any bucket, scope or collection, and either every write it
makes becomes visible or none does.  The ``couchbase`` and ``acouchbase`` APIs run transactions; the
``txcouchbase`` API does not.

This page covers how the SDK runs one.  :txns_examples:`The Couchbase documentation <>` covers what
transactions guarantee and how they work on the server.

.. contents::
    :local:

Running a transaction
=====================

A transaction is a function, the transaction's logic, which the cluster's
:attr:`~couchbase.cluster.Cluster.transactions` runs.  The logic is given an
:class:`~couchbase.transactions.transactions.AttemptContext`, and does its reads and writes through it:

.. code-block:: python

    from couchbase.transactions import AttemptContext

    collection = cluster.bucket('default').default_collection()

    def transfer(ctx: AttemptContext) -> None:
        source = ctx.get(collection, 'account-1')
        target = ctx.get(collection, 'account-2')
        source_content = source.content_as[dict]
        target_content = target.content_as[dict]
        source_content['balance'] -= 100
        target_content['balance'] += 100
        ctx.replace(source, source_content)
        ctx.replace(target, target_content)

    result = cluster.transactions.run(transfer)

:meth:`~couchbase.transactions.transactions.Transactions.run` returns once the transaction has committed, with a
:class:`~couchbase.transactions.TransactionResult`.  When the logic returns, the transaction commits.

The operations are :meth:`~couchbase.transactions.transactions.AttemptContext.get`,
:meth:`~couchbase.transactions.transactions.AttemptContext.get_multi`,
:meth:`~couchbase.transactions.transactions.AttemptContext.insert`,
:meth:`~couchbase.transactions.transactions.AttemptContext.replace`,
:meth:`~couchbase.transactions.transactions.AttemptContext.remove` and
:meth:`~couchbase.transactions.transactions.AttemptContext.query`.  ``replace`` and ``remove`` take the result of a
``get`` in the same transaction, not a key.

The logic can run more than once
--------------------------------

The SDK retries a transaction when an attempt runs into a conflict with another transaction, or a
transient error, and runs the logic again from the start, with a new context.  So the logic should do
nothing but read and write through the context: anything else it does, such as sending a message or
updating a variable outside it, happens once per attempt.

Errors
======

Raising an exception from the logic rolls the transaction back, and nothing it wrote is seen.  The SDK
does not retry it, and :meth:`~couchbase.transactions.transactions.Transactions.run` raises
:class:`~couchbase.exceptions.TransactionFailed`.

An operation's error, left to propagate, does the same.  ``inner_cause`` on the
:class:`~couchbase.exceptions.TransactionFailed` is the error that caused it:

.. code-block:: python

    from couchbase.exceptions import DocumentNotFoundException, TransactionFailed

    try:
        cluster.transactions.run(transfer)
    except TransactionFailed as ex:
        if isinstance(ex.inner_cause, DocumentNotFoundException):
            ...

The logic can catch an error from an operation that changed nothing and carry on, and the transaction
can still commit: :class:`~couchbase.exceptions.DocumentNotFoundException` from a ``get``,
:class:`~couchbase.exceptions.DocumentExistsException` from an ``insert``, or a query that fails to
parse:

.. code-block:: python

    def get_or_create(ctx: AttemptContext) -> None:
        try:
            ctx.get(collection, 'counter')
        except DocumentNotFoundException:
            ctx.insert(collection, 'counter', {'count': 0})

A :class:`~couchbase.exceptions.TransactionOperationFailed` is different: it fails the attempt even
when the logic catches it, so there is nothing to gain by catching one.

:meth:`~couchbase.transactions.transactions.Transactions.run` raises one of three exceptions, all of them
:class:`~couchbase.exceptions.TransactionException`:

:class:`~couchbase.exceptions.TransactionFailed`
    The transaction was rolled back.  Nothing it wrote is seen.

:class:`~couchbase.exceptions.TransactionExpired`
    The transaction ran out of time before it committed, and was rolled back.  Nothing it wrote is seen.

:class:`~couchbase.exceptions.TransactionCommitAmbiguous`
    The transaction ran out of time while it was committing, so whether it committed is not known.
    Background cleanup finishes it one way or the other.

Queries
=======

:meth:`~couchbase.transactions.transactions.AttemptContext.query` runs a SQL++ statement inside the transaction, and
can be mixed with the key-value operations.  Its writes commit or roll back with the rest:

.. code-block:: python

    from couchbase.options import TransactionQueryOptions

    def reprice(ctx: AttemptContext) -> None:
        result = ctx.query('UPDATE `travel-sample`.inventory.route SET price = price * 1.1 '
                           'WHERE sourceairport = $1',
                           TransactionQueryOptions(positional_parameters=['SFO']))
        for row in result.rows():
            ...

Configuration
=============

:class:`~couchbase.options.TransactionConfig`, given as ``transaction_config`` in
:class:`~couchbase.options.ClusterOptions`, configures every transaction the cluster runs: how long one
may take, its durability, where its metadata goes, and the background cleanup.
:class:`~couchbase.options.TransactionOptions`, passed to
:meth:`~couchbase.transactions.transactions.Transactions.run`, overrides some of it for one transaction:

.. code-block:: python

    from datetime import timedelta

    from couchbase.durability import DurabilityLevel, ServerDurability
    from couchbase.options import ClusterOptions, TransactionConfig, TransactionOptions

    config = TransactionConfig(durability=ServerDurability(DurabilityLevel.MAJORITY),
                               timeout=timedelta(seconds=30))
    cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth, transaction_config=config))

    cluster.transactions.run(transfer, TransactionOptions(timeout=timedelta(seconds=5)))

A transaction's timeout covers every attempt, and defaults to 15 seconds.

asyncio
=======

With ``acouchbase``, the logic is a coroutine function, and every operation on the context is awaited:

.. code-block:: python

    from acouchbase.transactions import AttemptContext

    async def transfer(ctx: AttemptContext) -> None:
        source = await ctx.get(collection, 'account-1')
        target = await ctx.get(collection, 'account-2')
        ...
        await ctx.replace(source, source_content)
        await ctx.replace(target, target_content)

    result = await cluster.transactions.run(transfer)

Everything else on this page holds for it as written.
