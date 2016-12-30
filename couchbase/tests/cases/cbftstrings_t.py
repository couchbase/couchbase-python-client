from __future__ import print_function

from couchbase.tests.base import CouchbaseTestCase
import couchbase.fulltext as cbft
from couchbase.n1ql import MutationState


class FTStringsTest(CouchbaseTestCase):
    def test_fuzzy(self):
        q = cbft.TermQuery('someterm', field='field', boost=1.5,
                           prefix_length=23, fuzziness=12)
        p = cbft.Params(explain=True)

        exp_json = {
            'query': {
                'term': 'someterm',
                'boost': 1.5,
                'fuzziness':  12,
                'prefix_length': 23,
                'field': 'field'
            },
            'indexName': 'someIndex',
            'explain': True
        }

        self.assertEqual(exp_json, cbft.make_search_body('someIndex', q, p))

    def test_match_phrase(self):
        exp_json = {
            'query': {
                'match_phrase': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field'
            },
            'size': 10,
            'indexName': 'ix'
        }

        p = cbft.Params(limit=10)
        q = cbft.MatchPhraseQuery('salty beers', boost=1.5, analyzer='analyzer',
                                  field='field')
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_match_query(self):
        exp_json = {
            'query': {
                'match': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field',
                'fuzziness': 1234,
                'prefix_length': 4
            },
            'size': 10,
            'indexName': 'ix'
        }

        q = cbft.MatchQuery('salty beers', boost=1.5, analyzer='analyzer',
                            field='field', fuzziness=1234, prefix_length=4)
        p = cbft.Params(limit=10)
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_string_query(self):
        exp_json = {
            'query': {
                'query': 'q*ry',
                'boost': 2.0,
            },
            'explain': True,
            'size': 10,
            'indexName': 'ix'
        }
        q = cbft.QueryStringQuery('q*ry', boost=2.0)
        p = cbft.Params(limit=10, explain=True)
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_params(self):
        self.assertEqual({}, cbft.Params().as_encodable('ix'))
        self.assertEqual({'size': 10}, cbft.Params(limit=10).as_encodable('ix'))
        self.assertEqual({'from': 100},
                         cbft.Params(skip=100).as_encodable('ix'))

        self.assertEqual({'explain': True},
                         cbft.Params(explain=True).as_encodable('ix'))

        self.assertEqual({'highlight': {'style': 'html'}},
                         cbft.Params(highlight_style='html').as_encodable('ix'))

        self.assertEqual({'highlight': {'style': 'ansi',
                                        'fields': ['foo', 'bar', 'baz']}},
                         cbft.Params(highlight_style='ansi',
                                     highlight_fields=['foo', 'bar', 'baz'])
                         .as_encodable('ix'))

        self.assertEqual({'fields': ['foo', 'bar', 'baz']},
                         cbft.Params(fields=['foo', 'bar', 'baz']
                                     ).as_encodable('ix'))

        self.assertEqual({'sort': ['f1', 'f2', '-_score']},
                         cbft.Params(sort=['f1', 'f2', '-_score']
                                     ).as_encodable('ix'))

        p = cbft.Params(facets={
            'term': cbft.TermFacet('somefield', limit=10),
            'dr': cbft.DateFacet('datefield').add_range('name', 'start', 'end'),
            'nr': cbft.NumericFacet('numfield').add_range('name2', 0.0, 99.99)
        })
        exp = {
            'facets': {
                'term': {
                    'field': 'somefield',
                    'size': 10
                },
                'dr': {
                    'field': 'datefield',
                    'date_ranges': [{
                        'name': 'name',
                        'start': 'start',
                        'end': 'end'
                    }]
                },
                'nr': {
                    'field': 'numfield',
                    'numeric_ranges': [{
                        'name': 'name2',
                        'min': 0.0,
                        'max': 99.99
                    }]
                },
            }
        }
        self.assertEqual(exp, p.as_encodable('ix'))

    def test_facets(self):
        p = cbft.Params()
        f = cbft.NumericFacet('numfield')
        self.assertRaises(ValueError, p.facets.__setitem__, 'facetName', f)
        self.assertRaises(TypeError, f.add_range, 'range1')
        p.facets['facetName'] = f.add_range('range1', min=123, max=321)
        self.assertTrue('facetName' in p.facets)

        f = cbft.DateFacet('datefield')
        f.add_range('r1', start='2012', end='2013')
        f.add_range('r2', start='2014')
        f.add_range('r3', end='2015')
        exp = {
            'field': 'datefield',
            'date_ranges': [
                {'name': 'r1', 'start': '2012', 'end': '2013'},
                {'name': 'r2', 'start': '2014'},
                {'name': 'r3', 'end': '2015'}
            ]
        }
        self.assertEqual(exp, f.encodable)

        f = cbft.TermFacet('termfield')
        self.assertEqual({'field': 'termfield'}, f.encodable)
        f.limit = 10
        self.assertEqual({'field': 'termfield', 'size': 10}, f.encodable)

    def test_raw_query(self):
        qq = cbft.RawQuery({'foo': 'bar'})
        self.assertEqual({'foo': 'bar'}, qq.encodable)

    def test_wildcard_query(self):
        qq = cbft.WildcardQuery('f*o', field='wc')
        self.assertEqual({'wildcard': 'f*o', 'field': 'wc'}, qq.encodable)

    def test_docid_query(self):
        qq = cbft.DocIdQuery([])
        self.assertRaises(cbft.NoChildrenError, getattr, qq, 'encodable')
        qq.ids = ['foo', 'bar', 'baz']
        self.assertEqual({'ids': ['foo', 'bar', 'baz']}, qq.encodable)

    def test_boolean_query(self):
        prefix_q = cbft.PrefixQuery('someterm', boost=2)
        bool_q = cbft.BooleanQuery(
            must=prefix_q, must_not=prefix_q, should=prefix_q)
        exp = {'prefix': 'someterm', 'boost': 2.0}
        self.assertEqual({'conjuncts': [exp]},
                         bool_q.must.encodable)
        self.assertEqual({'min': 1, 'disjuncts': [exp]},
                         bool_q.should.encodable)
        self.assertEqual({'min': 1, 'disjuncts': [exp]},
                         bool_q.must_not.encodable)

        # Test multiple criteria in must and must_not
        pq_1 = cbft.PrefixQuery('someterm', boost=2)
        pq_2 = cbft.PrefixQuery('otherterm')
        bool_q = cbft.BooleanQuery(must=[pq_1, pq_2])
        exp = {
            'conjuncts': [
                {'prefix': 'someterm', 'boost': 2.0},
                {'prefix': 'otherterm'}
            ]
        }
        self.assertEqual({'must': exp}, bool_q.encodable)

    def test_daterange_query(self):
        self.assertRaises(TypeError, cbft.DateRangeQuery)
        dr = cbft.DateRangeQuery(end='theEnd')
        self.assertEqual({'end': 'theEnd'}, dr.encodable)
        dr = cbft.DateRangeQuery(start='theStart')
        self.assertEqual({'start': 'theStart'}, dr.encodable)
        dr = cbft.DateRangeQuery(start='theStart', end='theEnd')
        self.assertEqual({'start': 'theStart', 'end': 'theEnd'}, dr.encodable)
        dr = cbft.DateRangeQuery('', '')  # Empty strings should be ok
        self.assertEqual({'start': '', 'end': ''}, dr.encodable)

    def test_numrange_query(self):
        self.assertRaises(TypeError, cbft.NumericRangeQuery)
        nr = cbft.NumericRangeQuery(0, 0)  # Should be OK
        self.assertEqual({'min': 0, 'max': 0}, nr.encodable)
        nr = cbft.NumericRangeQuery(0.1, 0.9)
        self.assertEqual({'min': 0.1, 'max': 0.9}, nr.encodable)
        nr = cbft.NumericRangeQuery(max=0.9)
        self.assertEqual({'max': 0.9}, nr.encodable)
        nr = cbft.NumericRangeQuery(min=0.1)
        self.assertEqual({'min': 0.1}, nr.encodable)

    def test_disjunction_query(self):
        dq = cbft.DisjunctionQuery()
        self.assertEqual(1, dq.min)
        self.assertRaises(cbft.NoChildrenError, getattr, dq, 'encodable')

        dq.disjuncts.append(cbft.PrefixQuery('somePrefix'))
        self.assertEqual({'min': 1, 'disjuncts': [{'prefix': 'somePrefix'}]},
                         dq.encodable)
        self.assertRaises(ValueError, setattr, dq, 'min', 0)
        dq.min = 2
        self.assertRaises(cbft.NoChildrenError, getattr, dq, 'encodable')

    def test_conjunction_query(self):
        cq = cbft.ConjunctionQuery()
        self.assertRaises(cbft.NoChildrenError, getattr, cq, 'encodable')
        cq.conjuncts.append(cbft.PrefixQuery('somePrefix'))
        self.assertEqual({'conjuncts': [{'prefix': 'somePrefix'}]},
                         cq.encodable)

    def test_match_all_none_queries(self):
        self.assertEqual({'match_all': None}, cbft.MatchAllQuery().encodable)
        self.assertEqual({'match_none': None}, cbft.MatchNoneQuery().encodable)

    def test_phrase_query(self):
        pq = cbft.PhraseQuery('salty', 'beers')
        self.assertEqual({'terms': ['salty', 'beers']}, pq.encodable)

        pq = cbft.PhraseQuery()
        self.assertRaises(cbft.NoChildrenError, getattr, pq, 'encodable')
        pq.terms.append('salty')
        self.assertEqual({'terms': ['salty']}, pq.encodable)

    def test_prefix_query(self):
        pq = cbft.PrefixQuery('someterm', boost=1.5)
        self.assertEqual({'prefix': 'someterm', 'boost': 1.5}, pq.encodable)

    def test_regexp_query(self):
        pq = cbft.RegexQuery('some?regex')
        self.assertEqual({'regexp': 'some?regex'}, pq.encodable)

    def test_booleanfield_query(self):
        bq = cbft.BooleanFieldQuery(True)
        self.assertEqual({'bool': True}, bq.encodable)

    def test_consistency(self):
        uuid = str('10000')
        vb = 42
        seq = 101
        ixname = 'ix'

        mutinfo = (vb, uuid, seq, 'dummy-bucket-name')
        ms = MutationState()
        ms._add_scanvec(mutinfo)

        params = cbft.Params()
        params.consistent_with(ms)
        got = cbft.make_search_body('ix', cbft.MatchNoneQuery(), params)
        exp = {
            'indexName': ixname,
            'query': {
                'match_none': None
            },
            'ctl': {
                'consistency': {
                    'level': 'at_plus',
                    'vectors': {
                        ixname: {
                            '{0}/{1}'.format(vb, uuid): seq
                        }
                    }
                }
            }
        }
        self.assertEqual(exp, got)