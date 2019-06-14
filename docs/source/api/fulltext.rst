#################
Full Text Queries
#################

.. module:: couchbase_core.fulltext

Full-text queries search the cluster for documents matching certain terms or
strings of texts. Unlike N1QL queries which are designed to be structured, i.e.
ensuring that a given document field matches a given value, full-text queries
are more free-form, allowing the ability to search multiple fields and to
add variance in queries.

Couchbase offers full-text queries in version 4.5. To issue a query from the
Python SDK, use the :cb_bmeth:`search` cluster method.

To perform an actual search, define a search index (via Couchbase web UI, or
using the REST interface), create a :class:`Query` object and then pass the
index name and the query object to the `search` method:

.. code-block:: python

    query = ConjunctionQuery(TermQuery("couchbase"), MatchQuery("nosql"))
    for hit in cluster.search('indexName', query):
        print(hit)


The above query searches for any document which has both `nosql` and `couchbase`
in *any* of its fields. Note that :class:`PhraseQuery` may be better suited for
this kind of query

.. code-block:: python

    query = PhraseQuery('couchbase', 'nosql')
    for hit in cluster.search('indexName', query):
        print(hit)

-----------
Query Types
-----------

You may issue simple match queries (:class:`MatchQuery`) to inspect a user
term; :class:`TermQuery` to match a field exactly, :class:`PrefixQuery` for
type-ahead queries, or a compound query type such as :class:`ConjunctionQuery`
for more complex queries.

.. autoclass:: Query
    :members:

=============
Match Queries
=============

.. autoclass:: MatchQuery
    :members:

.. autoclass:: MatchPhraseQuery
    :members:

.. autoclass:: PrefixQuery
    :members:

.. autoclass:: RegexQuery
    :members:

.. autoclass:: WildcardQuery
    :members:

.. autoclass:: BooleanFieldQuery
    :members:

.. autoclass:: QueryStringQuery
    :members:

.. autoclass:: DocIdQuery
    :members:

=============
Range Queries
=============

.. autoclass:: NumericRangeQuery
    :members:

.. autoclass:: DateRangeQuery
    :members:

================
Compound Queries
================

.. autoclass:: ConjunctionQuery
    :members:

.. autoclass:: DisjunctionQuery
    :members:

.. autoclass:: BooleanQuery
    :members:

=================
Debugging Queries
=================

.. autoclass:: MatchAllQuery
    :members:

.. autoclass:: MatchNoneQuery
    :members:

.. autoclass:: TermQuery
    :members:

.. autoclass:: PhraseQuery
    :members:


----------
Parameters
----------

Query parameters may be passed as the ``params`` keyword argument to
:cb_bmeth:`search`.

.. autoclass:: Params
    :members:

------
Facets
------

Facets allow additional aggregate information to be returned in the
results. You can count how many documents match specific criteria
based on ranges and matches.

.. autoclass:: TermFacet
    :members:

.. autoclass:: DateFacet
    :members:

.. autoclass:: NumericFacet
    :members:
