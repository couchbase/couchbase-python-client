import abc


from couchbase_core.exceptions import CouchbaseError
from couchbase_core.views.iterator import AlreadyQueriedError
from couchbase_core import _to_json
from couchbase_core._pyport import unicode


def _genprop(converter, *apipaths, **kwargs):
    """
    This internal helper method returns a property (similar to the
    @property decorator). In additional to a simple Python property,
    this also adds a type validator (`converter`) and most importantly,
    specifies the path within a dictionary where the value should be
    stored.

    Any object using this property *must* have a dictionary called ``_json``
    as a property

    :param converter: The converter to be used, e.g. `int` or `lambda x: x`
        for passthrough
    :param apipaths:
        Components of a path to store, e.g. `foo, bar, baz` for
            `foo['bar']['baz']`
    :return: A property object
    """
    if not apipaths:
        raise TypeError('Must have at least one API path')

    def fget(self):
        d = self._json_
        try:
            for x in apipaths:
                d = d[x]
            return d
        except KeyError:
            return None

    def fset(self, value):
        value = converter(value)
        d = self._json_
        for x in apipaths[:-1]:
            d = d.setdefault(x, {})
        d[apipaths[-1]] = value

    def fdel(self):
        d = self._json_
        try:
            for x in apipaths[:-1]:
                d = d[x]
            del d[apipaths[-1]]
        except KeyError:
            pass

    doc = kwargs.pop(
        'doc', 'Corresponds to the ``{0}`` field'.format('.'.join(apipaths)))
    return property(fget, fset, fdel, doc)


def _genprop_str(*apipaths, **kwargs):
    """
    Convenience function to return a string property in which the value
    is converted to a string
    """
    return _genprop(unicode, *apipaths, **kwargs)


def _highlight(value):
    """
    highlight 'type validator'.
    """
    if value not in ('html', 'ansi'):
        raise ValueError(
            'Highlight must be "html" or "ansi", got {0}'.format(value))
    return value

def _consistency(value):
    """
    Validator for 'consistency' parameter
    """
    if value and value.lower() not in ('', 'not_bounded'):
        raise ValueError('Invalid value!')
    return value

def _assign_kwargs(self, kwargs):
    """
    Assigns all keyword arguments to a given instance, raising an exception
    if one of the keywords is not already the name of a property.
    """
    for k in kwargs:
        if not hasattr(self, k):
            raise AttributeError(k, 'Not valid for', self.__class__.__name__)
        setattr(self, k, kwargs[k])


class Facet(object):
    """
    Base facet class. Each facet must have a field which it aggregates
    """
    def __init__(self, field):
        self._json_ = {'field': field}

    @property
    def encodable(self):
        """
        Returns a reprentation of the object suitable for serialization
        """
        return self._json_

    """
    Field upon which the facet will aggregate
    """
    field = _genprop_str('field')

    def __repr__(self):
        return '{0.__class__.__name__}<{0._json_!r}>'.format(self)


class TermFacet(Facet):
    """
    Facet aggregating the most frequent terms used.
    """
    def __init__(self, field, limit=0):
        super(TermFacet, self).__init__(field)
        if limit:
            self.limit = limit

    limit = _genprop(int, 'size',
                     doc="Maximum number of terms/count pairs to return")


def _mk_range_bucket(name, n1, n2, r1, r2):
    """
    Create a named range specification for encoding.

    :param name: The name of the range as it should appear in the result
    :param n1: The name of the lower bound of the range specifier
    :param n2: The name of the upper bound of the range specified
    :param r1: The value of the lower bound (user value)
    :param r2: The value of the upper bound (user value)
    :return: A dictionary containing the range bounds. The upper and lower
        bounds are keyed under ``n1`` and ``n2``.

    More than just a simple wrapper, this will not include any range bound
    which has a user value of `None`. Likewise it will raise an exception if
    both range values are ``None``.
    """
    d = {}
    if r1 is not None:
        d[n1] = r1
    if r2 is not None:
        d[n2] = r2
    if not d:
        raise TypeError('Must specify at least one range boundary!')
    d['name'] = name
    return d


class DateFacet(Facet):
    """
    Facet to aggregate results based on a date range.
    This facet must have at least one invocation of :meth:`add_range` before
    it is added to :attr:`~Params.facets`.
    """
    def __init__(self, field):
        super(DateFacet, self).__init__(field)
        self._ranges = []

    def add_range(self, name, start=None, end=None):
        """
        Adds a date range to the given facet.

        :param str name:
            The name by which the results within the range can be accessed
        :param str start: Lower date range. Should be in RFC 3339 format
        :param str end: Upper date range.
        :return: The `DateFacet` object, so calls to this method may be
            chained
        """
        self._ranges.append(_mk_range_bucket(name, 'start', 'end', start, end))
        return self

    _ranges = _genprop(list, 'date_ranges')


class NumericFacet(Facet):
    """
    Facet to aggregate results based on a numeric range.
    This facet must have at least one invocation of :meth:`add_range`
    before it is added to :attr:`Params.facets`
    """
    def __init__(self, field):
        super(NumericFacet, self).__init__(field)
        self._ranges = []

    def add_range(self, name, min=None, max=None):
        """
        Add a numeric range.

        :param str name:
            the name by which the range is accessed in the results
        :param int | float min: Lower range bound
        :param int | float max: Upper range bound
        :return: This object; suitable for method chaining
        """
        self._ranges.append(_mk_range_bucket(name, 'min', 'max', min, max))
        return self

    _ranges = _genprop(list, 'numeric_ranges')


class _FacetDict(dict):
    """
    Internal dict subclass which ensures that facets added to this dictionary
    have properly defined ranges.
    """

    # noinspection PyMissingConstructor
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        if not isinstance(value, Facet):
            raise ValueError('Can only add facet')
        if hasattr(value, '_ranges') and not getattr(value, '_ranges'):
            raise ValueError('{} object must have at least one range. Use '
                             'add_range'.format(value.__class__.__name__))
        super(_FacetDict, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError('only one merge at a time!')
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        for key in kwargs:
            self[key] = kwargs[key]

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        return self[key]


class Sort(object):
    def __init__(self, by, **kwargs):
        by = by.replace('descending','desc')
        self._json_ = {
            'by': by
        }
        if 'descending' in kwargs:
            kwargs['desc']=kwargs.pop('descending')
        _assign_kwargs(self, kwargs)

    desc = _genprop(bool, 'desc', doc='Sort using descending order')

    @property
    def descending(self):
        return self.desc

    @descending.setter
    def descending(self, value):
        self.desc = value

    def as_encodable(self):
        return self._json_


class SortString(Sort):
    """
    Sorts by a list of fields. This is similar to specifying a list of
    fields in :attr:`Params.sort`
    """
    def __init__(self, *fields):
        self._json_ = list(fields)


class SortScore(Sort):
    """
    Sorts by the score of each match.
    """
    def __init__(self, **kwargs):
        super(SortScore, self).__init__('score', **kwargs)


class SortID(Sort):
    """
    Sorts lexically by the document ID of each match
    """
    def __init__(self, **kwargs):
        super(SortID, self).__init__('id', **kwargs)


class SortField(Sort):
    """
    Sorts according to the properties of a given field
    """
    def __init__(self, field, **kwargs):
        kwargs['field'] = field
        super(SortField, self).__init__('field', **kwargs)

    field = _genprop_str('field', doc='Field to sort by')
    type = _genprop_str('type', doc='Coerce field to this type')
    mode = _genprop_str('mode')
    missing = _genprop_str('missing')


def _location_conv(l):
    if len(l) != 2:
        raise ValueError('Require list of two numbers')
    return [float(l[0]), float(l[1])]


class SortGeoDistance(Sort):
    """
    Sorts matches based on their distance from a specific location
    """
    def __init__(self, location, field, **kwargs):
        kwargs.update(location=location, field=field)
        super(SortGeoDistance, self).__init__('geo_distance', **kwargs)

    location = _genprop(_location_conv, 'location',
                        doc='`(lat, lon)` of point of origin')
    field = _genprop_str('field', doc='Field that contains the distance')
    unit = _genprop_str('unit', doc='Distance unit used for measuring')


class SortRaw(Sort):
    def __init__(self, raw):
        self._json_ = raw


def _convert_sort(s):
    if isinstance(s, Sort):
        return s
    else:
        return list(s)


class Params(object):
    """
    Generic parameters and query modifiers. Keyword arguments may be used
    to initialize instance attributes. See individual attributes for
    more information.

    .. attribute:: facets

        A dictionary of :class:`Facet` objects. The dictionary uses the
        facet name for keys. You can retrieve the facet results by their name
        as well.

        You can add facets like so::

            params.facets['term_analysis'] = TermFacet('author', limit=10)
            params.facets['view_count'] = NumericFacet().add_range('low', max=50)
    """
    def __init__(self, **kwargs):
        self._json_ = {}
        self._ms = None
        self.facets = _FacetDict(**kwargs.pop('facets', {}))
        _assign_kwargs(self, kwargs)

    def as_encodable(self, index_name):
        """
        :param index_name: The name of the index for the query
        :return: A dict suitable for passing to `json.dumps()`
        """
        if self.facets:
            encoded_facets = {}
            for name, facet in self.facets.items():
                encoded_facets[name] = facet.encodable
            self._json_['facets'] = encoded_facets

        if self._ms:
            # Encode according to scan vectors..
            sv_val = {
                'level': 'at_plus',
                'vectors': {
                    index_name: self._ms._to_fts_encodable()
                }
            }
            self._json_.setdefault('ctl', {})['consistency'] = sv_val

        if self.sort:
            if isinstance(self.sort, Sort):
                self._json_['sort'] = self.sort.as_encodable()
            else:
                self._json_['sort'] = self.sort

        return self._json_

    limit = _genprop(int, 'size', doc='Maximum number of results to return')

    skip = _genprop(int, 'from', doc='Seek by this number of results')

    explain = _genprop(
        bool, 'explain', doc='Whether to return the explanation of the search')

    fields = _genprop(
        list, 'fields', doc='Return these fields for each document')

    timeout = _genprop(lambda x: int(x * 1000), 'ctl', 'timeout',
                       doc='Timeout for the query, in seconds')

    highlight_style = _genprop(_highlight, 'highlight', 'style', doc="""
        Highlight the results using a given style.
        Can be either `ansi` or `html`
        """)

    highlight_fields = _genprop(
        list, 'highlight', 'fields', doc="""
        Highlight the results from these fields (list)
        """)

    sort = _genprop(
        _convert_sort, 'sort', doc="""
        Specify a list of fields by which to sort the results. Can also be
        a :class:`Sort` class
        """
    )

    consistency = _genprop(
        _consistency, 'ctl', 'consistency', doc="""
        Consistency for the query. Use this for 'fixed' consistency, or to
        clear consistency.

        You might want to use :meth:`consistent_with` for consistency that is
        bounded to specific mutations
        """
    )

    def consistent_with(self, ms):
        """
        Ensure that this query is consistent with the given mutations. When
        set, this ensures that only document versions as or more recent than
        the provided mutations are used for the search. This is often helpful
        when attempting searches on newly inserted documents.
        :param ms: Mutation State
        :type ms: :class:`couchbase_core.mutation_state.MutationState`
        """
        if self.consistency:
            raise ValueError(
                'Clear "consistency" before specifying "consistent_with"')
        self._ms = ms


class Query(object):
    """
    Base query object. You probably want to use one of the subclasses.

    .. seealso:: :class:`MatchQuery`, :class:`BooleanQuery`,
        :class:`RegexQuery`, :class:`PrefixQuery`, :class:`NumericRangeQuery`,
        :class:`DateRangeQuery`, :class:`ConjunctionQuery`,
        :class:`DisjunctionQuery`, and others in this module.
    """
    def __init__(self):
        self._json_ = {}

    boost = _genprop(
        float, 'boost', doc="""
        When specifying multiple queries, you can give this query a
        higher or lower weight in order to prioritize it among sibling
        queries. See :class:`ConjunctionQuery`
        """)

    @property
    def encodable(self):
        """
        Returns an object suitable for serializing to JSON

        .. code-block:: python

            json.dumps(query.encodable)
        """
        self.validate()
        return self._json_

    def validate(self):
        """
        Optional validation function. Invoked before encoding
        """
        pass


class RawQuery(Query):
    """
    This class is used to wrap a raw query payload. It should be used
    for custom query parameters, or in cases where any of the other
    query classes are insufficient.
    """
    def __init__(self, obj):
        super(RawQuery, self).__init__()
        self._json_ = obj


class _SingleQuery(Query):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def _TERMPROP(self):
        """Name of the JSON property that contains the mandatory match spec"""

    def __init__(self, term, **kwargs):
        super(_SingleQuery, self).__init__()
        if self._TERMPROP not in kwargs:
            kwargs[self._TERMPROP] = term
        _assign_kwargs(self, kwargs)


_COMMON_FIELDS = {
    'prefix_length': _genprop(
        int, 'prefix_length',
        doc="""
        When using :attr:`fuzziness`, this controls how much of the term
        or phrase is *excluded* from fuzziness. This may help improve
        performance at the expense of omitting potential fuzzy matches
        at the beginning of the string.
        """),
    'fuzziness': _genprop(
        int, 'fuzziness',
        doc="""
        Allow a given degree of fuzz in the match. Matches which are closer to
        the original term will be scored higher.

        You can apply the fuzziness to only a portion of the string by
        specifying :attr:`prefix_length` - indicating that only the part
        of the field after the prefix length should be checked with fuzziness.

        This value is specified as a float
        """),
    'field': _genprop(
        str, 'field',
        doc="""
        Restrict searching to a given document field
        """),
    'analyzer': _genprop(
        str, 'analyzer',
        doc="""
        Use a defined server-side analyzer to process the input term prior to
        executing the search
        """
    )
}


def _with_fields(*fields):
    """
    Class decorator to include common query fields
    :param fields: List of fields to include. These should be keys of the
        `_COMMON_FIELDS` dict
    """
    dd = {}
    for x in fields:
        dd[x] = _COMMON_FIELDS[x]

    def wrap(cls):
        dd.update(cls.__dict__)
        return type(cls.__name__, cls.__bases__, dd)

    return wrap


class QueryStringQuery(_SingleQuery):
    """
    Query which allows users to describe a query in a query language.
    The server will then execute the appropriate query based on the contents
    of the query string:

    .. seealso::

        `Query Language <http://www.blevesearch.com/docs/Query-String-Query/>`_

    Example::

        QueryStringQuery('description:water and stuff')
    """

    _TERMPROP = 'query'
    query = _genprop_str('query')

    """
    Actual query string
    """


@_with_fields('field')
class WildcardQuery(_SingleQuery):
    """
    Query in which the characters `*` and `?` have special meaning, where
    `?` matches 1 occurrence and `*` will match 0 or more occurrences of the
    previous character
    """
    _TERMPROP = 'wildcard'
    wildcard = _genprop_str(_TERMPROP, doc='Wildcard pattern to use')


def _list_nonempty_conv(l):
    if not l:
        raise ValueError('Must have at least one value')


class DocIdQuery(_SingleQuery):
    """
    Matches document IDs. This is must useful in a compound query
    (for example, :class:`BooleanQuery`). When used as a criteria, only
    documents with the specified IDs will be searched.
    """
    _TERMPROP = 'ids'
    ids = _genprop(list, 'ids', doc="""
    List of document IDs to use
    """)

    def validate(self):
        super(DocIdQuery, self).validate()
        if not self.ids:
            raise NoChildrenError('`ids` must contain at least one ID')


@_with_fields('prefix_length', 'fuzziness', 'field', 'analyzer')
class MatchQuery(_SingleQuery):
    """
    Query which checks one or more fields for a match
    """
    _TERMPROP = 'match'
    match = _genprop_str(
        'match', doc="""
        String to search for
        """)


@_with_fields('fuzziness', 'prefix_length', 'field')
class TermQuery(_SingleQuery):
    """
    Searches for a given term in documents. Unlike :class:`MatchQuery`,
    the term is not analyzed.

    Example::

        TermQuery('lcb_cntl_string')
    """
    _TERMPROP = 'term'
    term = _genprop_str('term', doc='Exact term to search for')


@_with_fields('field', 'analyzer')
class MatchPhraseQuery(_SingleQuery):
    """
    Search documents which match a given phrase. The phrase is composed
    of one or more terms.

    Example::

        MatchPhraseQuery("Hello world!")
    """
    _TERMPROP = 'match_phrase'
    match_phrase = _genprop_str(_TERMPROP, doc="Phrase to search for")


@_with_fields('field')
class PhraseQuery(_SingleQuery):
    _TERMPROP = 'terms'
    terms = _genprop(list, 'terms', doc='List of terms to search for')

    def __init__(self, *phrases, **kwargs):
        super(PhraseQuery, self).__init__(phrases, **kwargs)

    def validate(self):
        super(PhraseQuery, self).validate()
        if not self.terms:
            raise NoChildrenError('Missing terms')


@_with_fields('field')
class PrefixQuery(_SingleQuery):
    """
    Search documents for fields beginning with a certain prefix. This is
    most useful for type-ahead or lookup queries.
    """
    _TERMPROP = 'prefix'
    prefix = _genprop_str('prefix', doc='The prefix to match')


@_with_fields('field')
class RegexQuery(_SingleQuery):
    """
    Search documents for fields matching a given regular expression
    """
    _TERMPROP = 'regex'
    regex = _genprop_str('regexp', doc="Regular expression to use")


RegexpQuery = RegexQuery


@_with_fields('field')
class GeoDistanceQuery(Query):
    def __init__(self, distance, location, **kwargs):
        """
        Search for items within a given radius
        :param distance: The distance string specifying the radius
        :param location: A tuple of `(lon, lat)` indicating point of origin
        """
        super(GeoDistanceQuery, self).__init__()
        kwargs['distance'] = distance
        kwargs['location'] = location
        _assign_kwargs(self, kwargs)

    location = _genprop(_location_conv, 'location', doc='Location')
    distance = _genprop_str('distance')


@_with_fields('field')
class GeoBoundingBoxQuery(Query):
    def __init__(self, top_left, bottom_right, **kwargs):
        super(GeoBoundingBoxQuery, self).__init__()
        kwargs['top_left'] = top_left
        kwargs['bottom_right'] = bottom_right
        _assign_kwargs(self, kwargs)

    top_left = _genprop(
        _location_conv, 'top_left',
        doc='Tuple of `(lat, lon)` for the top left corner of bounding box')
    bottom_right = _genprop(
        _location_conv, 'bottom_right',
        doc='Tuple of `(lat, lon`) for the bottom right corner of bounding box')


class _RangeQuery(Query):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def _MINMAX(self):
        return 'min_name', 'max_name'

    def __init__(self, r1, r2, **kwargs):
        super(_RangeQuery, self).__init__()
        _assign_kwargs(self, kwargs)
        if r1 is None and r2 is None:
            raise TypeError('At least one of {0} or {1} should be specified',
                             *self._MINMAX)
        if r1 is not None:
            setattr(self, self._MINMAX[0], r1)
        if r2 is not None:
            setattr(self, self._MINMAX[1], r2)


@_with_fields('field')
class NumericRangeQuery(_RangeQuery):
    """
    Search documents for fields containing a value within a given numerical
    range.

    At least one of `min` or `max` must be specified.
    """
    def __init__(self, min=None, max=None, **kwargs):
        """
        :param float min: See :attr:`min`
        :param float max: See :attr:`max`
        """
        super(NumericRangeQuery, self).__init__(min, max, **kwargs)

    min = _genprop(
        float, 'min', doc='Lower bound of range. See :attr:`min_inclusive`')

    min_inclusive = _genprop(
        bool, 'inclusive_min',
        doc='Whether matches are inclusive of lower bound')

    max = _genprop(
        float, 'max',
        doc='Upper bound of range. See :attr:`max_inclusive`')

    max_inclusive = _genprop(
        bool, 'inclusive_max',
        doc='Whether matches are inclusive of upper bound')

    _MINMAX = 'min', 'max'


@_with_fields('field')
class DateRangeQuery(_RangeQuery):
    """
    Search documents for fields containing a value within a given date
    range.

    The date ranges are parsed according to a given :attr:`datetime_parser`.
    If no parser is specified, the RFC 3339 parser is used. See
    `Generating an RFC 3339 Timestamp <http://goo.gl/LIkV7G>_`.

    The :attr:`start` and :attr:`end` parameters should be specified in the
    constructor. Note that either `start` or `end` (but not both!) may be
    omitted.

    .. code-block:: python

        DateRangeQuery(start='2014-12-25', end='2016-01-01')
    """
    def __init__(self, start=None, end=None, **kwargs):
        """
        :param str start: Start of date range
        :param str end: End of date range
        :param kwargs: Additional options: :attr:`field`, :attr:`boost`
        """
        super(DateRangeQuery, self).__init__(start, end, **kwargs)

    start = _genprop_str('start', doc='Lower bound datetime')
    end = _genprop_str('end', doc='Upper bound datetime')

    start_inclusive = _genprop(
        bool, 'inclusive_start', doc='If :attr:`start` is inclusive')

    end_inclusive = _genprop(
        bool, 'inclusive_end', doc='If :attr:`end` is inclusive')

    datetime_parser = _genprop_str(
        'datetime_parser',
        doc="""
        Parser to use when analyzing the :attr:`start` and :attr:`end` fields
        on the server.

        If not specified, the RFC 3339 parser is used.
        Ensure to specify :attr:`start` and :attr:`end` in a format suitable
        for the given parser.
        """)

    _MINMAX = 'start', 'end'


@_with_fields('field')
class TermRangeQuery(_RangeQuery):
    """
    Search documents for fields containing a value within a given
    lexical range.
    """
    def __init__(self, start=None, end=None, **kwargs):
        super(TermRangeQuery, self).__init__(start=start, end=end, **kwargs)

    start = _genprop_str('start', doc='Lower range of term')

    end = _genprop_str('end', doc='Upper range of term')

    start_inclusive = _genprop(
        bool, 'inclusive_start', doc='If :attr:`start` is inclusive')

    end_inclusive = _genprop(
        bool, 'inclusive_end', doc='If :attr:`end` is inclusive')

    _MINMAX = 'start', 'end'


class _CompoundQuery(Query):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def _COMPOUND_FIELDS(self):
        """
        Field to contain the compound queries. should return an iterable of
        `(srcname, tgtname)` of the attribute name and the target JSON
         field that contains the actual list of queries
        """
        return ('Dummy', 'Dummy'),

    def __init__(self, **kwargs):
        super(_CompoundQuery, self).__init__()
        _assign_kwargs(self, kwargs)

    @property
    def encodable(self):
        self.validate()

        # Requires special handling since the compound queries in themselves
        # cannot be JSON unless they are properly encoded.
        # Note that the 'encodable' property also triggers validation
        js = self._json_.copy()
        for src, tgt in self._COMPOUND_FIELDS:
            objs = getattr(self, src)
            if not objs:
                continue
            js[tgt] = [q.encodable for q in objs]
        return js


class ConjunctionQuery(_CompoundQuery):
    """
    Compound query in which all sub-queries passed must be satisfied
    """
    _COMPOUND_FIELDS = ('conjuncts', 'conjuncts'),

    def __init__(self, *queries):
        super(ConjunctionQuery, self).__init__()
        self.conjuncts = list(queries)

    def validate(self):
        super(ConjunctionQuery, self).validate()
        if not self.conjuncts:
            raise NoChildrenError('No sub-queries')


def _convert_gt0(value):
    # Ensure value is greater than 0
    value = int(value)
    if not value:
        raise ValueError('Must be > 0')
    return value


class DisjunctionQuery(_CompoundQuery):
    """
    Compound query in which at least :attr:`min` or more queries must be
    satisfied
    """
    _COMPOUND_FIELDS = ('disjuncts', 'disjuncts'),

    def __init__(self, *queries, **kwargs):
        super(DisjunctionQuery, self).__init__()
        _assign_kwargs(self, kwargs)
        self.disjuncts = list(queries)
        if 'min' not in self._json_:
            self.min = 1

    min = _genprop(
        _convert_gt0, 'min', doc='Number of queries which must be satisfied')

    def validate(self):
        super(DisjunctionQuery, self).validate()
        if not self.disjuncts or len(self.disjuncts) < self.min:
            raise NoChildrenError('No children specified, or min is too big')


def _bprop_wrap(name, reqtype, doc):
    """
    Helper function to generate properties
    :param name: The name of the subfield in the JSON dictionary
    :param reqtype: The compound query type the query
        list should be coerced into
    :param doc: Documentation for the field
    :return: the property.
    """
    def fget(self):
        return self._subqueries.get(name)

    def fset(self, value):
        if value is None:
            if name in self._subqueries:
                del self._subqueries[name]
        elif isinstance(value, reqtype):
            self._subqueries[name] = value
        elif isinstance(value, Query):
            self._subqueries[name] = reqtype(value)
        else:
            try:
                it = iter(value)
            except ValueError:
                raise TypeError('Value must be instance of Query')

            l = []
            for q in it:
                if not isinstance(q, Query):
                    raise TypeError('Item is not a query!', q)
                l.append(q)
            self._subqueries[name] = reqtype(*l)

    def fdel(self):
        setattr(self, name, None)

    return property(fget, fset, fdel, doc)


class BooleanQuery(Query):
    def __init__(self, must=None, should=None, must_not=None):
        super(BooleanQuery, self).__init__()
        self._subqueries = {}
        self.must = must
        self.should = should
        self.must_not = must_not

    must = _bprop_wrap(
        'must', ConjunctionQuery,
        """
        Queries which must be satisfied. When setting this attribute, the
        SDK will convert value to a :class:`ConjunctionQuery` if the value
        is a list of queries.
        """)

    must_not = _bprop_wrap(
        'must_not', DisjunctionQuery,
        """
        Queries which must not be satisfied. Documents found which satisfy
        the queries in this clause are not returned in the match.

        When setting this attribute in the SDK, it will be converted to a
        :class:`DisjunctionQuery` if the value is a list of queries.
        """)

    should = _bprop_wrap(
        'should', DisjunctionQuery,
        """
        Specify additional queries which should be satisfied. As opposed to
        :attr:`must`, you can specify the number of queries in this field
        which must be satisfied.

        The type of this attribute is :class:`DisjunctionQuery`, and you can
        set the minimum number of queries to satisfy using::

            boolquery.should.min = 1
        """)

    @property
    def encodable(self):
        # Overrides the default `encodable` implementation in order to
        # serialize any sub-queries
        for src, tgt in ((self.must, 'must'),
                         (self.must_not, 'must_not'),
                         (self.should, 'should')):
            if src:
                self._json_[tgt] = src.encodable
        return super(BooleanQuery, self).encodable

    def validate(self):
        super(BooleanQuery, self).validate()
        if not self.must and not self.must_not and not self.should:
            raise ValueError('No sub-queries specified', self)


class MatchAllQuery(Query):
    """
    Special query which matches all documents
    """
    def __init__(self, **kwargs):
        super(MatchAllQuery, self).__init__()
        self._json_['match_all'] = None
        _assign_kwargs(self, kwargs)


class MatchNoneQuery(Query):
    """
    Special query which matches no documents
    """
    def __init__(self, **kwargs):
        super(MatchNoneQuery, self).__init__()
        self._json_['match_none'] = None
        _assign_kwargs(self, kwargs)


@_with_fields('field')
class BooleanFieldQuery(_SingleQuery):
    _TERMPROP = 'bool'
    bool = _genprop(bool, 'bool', doc='Boolean value to search for')


class SearchError(CouchbaseError):
    """
    Error during server execution
    """


class NoChildrenError(CouchbaseError):
    """
    Compound query is missing children"
    """
    def __init__(self, msg='No child queries'):
        super(NoChildrenError, self).__init__({'message': msg})


def make_search_body(index, query, params=None):
    """
    Generates a dictionary suitable for encoding as the search body
    :param index: The index name to query
    :param query: The query itself
    :param params: Modifiers for the query
    :type params: :class:`couchbase_core.fulltext.Params`
    :return: A dictionary suitable for serialization
    """
    dd = {}

    if not isinstance(query, Query):
        query = QueryStringQuery(query)

    dd['query'] = query.encodable
    if params:
        dd.update(params.as_encodable(index))
    dd['indexName'] = index
    return dd


class SearchRequest(object):
    """
    Object representing an active query on the cluster.

    .. warning::

        You should call :cb_bmeth:`search` which will return an instance of
        this class. Do *not* invoke the constructor directly.

    You can iterate over this object (i.e. ``__iter__``) to receive the
    actual search results.
    """
    def __init__(self, body, parent, row_factory=lambda x: x):
        """
        :param str body: serialized JSON string
        :param Bucket parent:
        """
        self._body = _to_json(body)
        self._parent = parent
        self.row_factory = row_factory
        self.errors = []
        self._mres = None
        self._do_iter = True
        self.__raw = False
        self.__meta_received = False

    @classmethod
    def mk_kwargs(cls, kwargs):
        """
        Pop recognized arguments from a keyword list.
        """
        ret = {}
        kws = ['row_factory', 'body', 'parent']
        for k in kws:
            if k in kwargs:
                ret[k] = kwargs.pop(k)

        return ret

    def _start(self):
        if self._mres:
            return

        self._mres = self._parent._fts_query(self._body)
        self.__raw = self._mres[None]

    @property
    def raw(self):
        return self.__raw

    def execute(self):
        """
        Use this convenience method if you are not actually reading the
        search hits, for example if you are only using :attr:`facets`.

        Equivalent to::

            def execute(self):
                [x for x in self]
                return self

        :return: :class:`SearchRequest` (self)
        """
        for _ in self:
            pass
        return self

    @property
    def meta(self):
        """
        Get metadata from the query itself. This is guaranteed to only
        return a Python dictionary.

        Note that if the query failed, the metadata might not be in JSON
        format, in which case there may be additional, non-JSON data
        which can be retrieved using the following

        ::

            raw_meta = req.raw.value

        :return: A dictionary containing the query metadata
        """
        if not self.__meta_received:
            raise RuntimeError(
                'This property only valid once all rows are received!')

        if isinstance(self.raw.value, dict):
            return self.raw.value
        return {}

    @property
    def total_hits(self):
        """
        The total number of hits that match the query. This may be greater
        than the number of hits actually returned, as it is subject to the
        :attr:`Params.limit` parameter
        """
        return self.meta['total_hits']

    @property
    def took(self):
        """
        The length of time the query took to execute
        """
        return self.meta['took']

    @property
    def max_score(self):
        return self.meta['max_score']

    @property
    def facets(self):
        return self.meta['facets']

    def _clear(self):
        del self._parent
        del self._mres

    def _handle_meta(self, value):
        self.__meta_received = True
        if not isinstance(value, dict):
            return
        if 'errors' in value:
            for err in value['errors']:
                raise SearchError.pyexc('N1QL Execution failed', err)

    def _process_payload(self, rows):
        if rows:
            return [self.row_factory(row) for row in rows]

        elif self.raw.done:
            self._handle_meta(self.raw.value)
            self._do_iter = False
            return []
        else:
            # We can only get here if another concurrent query broke out the
            # event loop before we did.
            return []

    def __iter__(self):
        if not self._do_iter:
            raise AlreadyQueriedError()

        self._start()
        while self._do_iter:
            raw_rows = self.raw.fetch(self._mres)
            for row in self._process_payload(raw_rows):
                yield row

    def __repr__(self):
        return (
            '<{0.__class__.__name__} body={0._body!r} response={1}>'.format(
                self, self.raw.value if self.raw else '<PENDING>'))
