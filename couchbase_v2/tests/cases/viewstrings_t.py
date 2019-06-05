#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# These tests are largely ported from php-ext-couchbase
import json

from couchbase_v2.views.params import (make_options_string,
                                       Query,
                                       ulp,
                                       UNSPEC,
                                       _HANDLER_MAP)

from couchbase_v2.exceptions import ArgumentError
from couchbase_tests.base import CouchbaseTestCase

class ViewStringTest(CouchbaseTestCase):
    def setUp(self):
        super(ViewStringTest, self).setUp()

    def _assert_vopteq(self, expected, key, value):
        s = make_options_string({key:value})
        self.assertEqual(s, expected)

    def _assert_vopteq_multi(self, d, key, value):
        q = Query(**{key:value})
        enc = q.encoded
        res = {}
        for kvp in enc.split("&"):
            k, v = kvp.split("=")
            res[k] = v

        d = d.copy()
        for k in d:
            d[k] = ulp.quote(d[k])

        self.assertEqual(res, d)

    def test_stale_params(self):
        self._assert_vopteq('stale=ok', 'stale', True)
        self._assert_vopteq('stale=false', 'stale', False)
        self._assert_vopteq('stale=update_after', "stale", "update_after")
        self._assert_vopteq('stale=ok', 'stale', 1)
        self._assert_vopteq('stale=false', 'stale', 0)
        self._assert_vopteq('stale=false', 'stale', "false")


    def test_bad_stale(self):
        self.assertRaises(ArgumentError,
                          self._assert_vopteq,
                          'stale=blahblah', 'stale', 'blahblha')
        self.assertRaises(ArgumentError,
                          self._assert_vopteq,
                          'stale=None', 'stale', None)


    def test_unrecognized_params(self):
        self.assertRaises(ArgumentError,
                          self._assert_vopteq,
                          'frobble=gobble', 'frobble', 'gobble')

    def test_misc_booleans(self):
        bparams = ('descending',
                   'reduce',
                   'inclusive_end',
                   'full_set',
                   'group')

        for p in bparams:
            # with string "false"
            self._assert_vopteq(p+"=false",
                                p,
                                "false")

            # with string "true"
            self._assert_vopteq(p+"=true",
                                p,
                                "true")

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              p+'=gobble', p, 'gobble')

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              p+'=None', p, None)

    def test_misc_numeric(self):
        nparams = (
            'connection_timeout',
            'group_level',
            'skip')

        for p in nparams:
            self._assert_vopteq(p+'=42',
                                p,
                                42)

            self._assert_vopteq(p+'=42',
                                p,
                                "42")

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              p+'=true', p, True)

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              p+'=blah', p, 'blah')

            self._assert_vopteq(p+'=0', p, 0)
            self._assert_vopteq(p+'=0', p, "0")
            self._assert_vopteq(p+'=-1', p, -1)

    def test_encode_string_to_json(self):
        jparams = (
            'endkey',
            'key',
            'startkey')

        values = (
            'dummy',
            42,
            None,
            True,
            False,
            { "chicken" : "broth" },
            ["noodle", "soup"],
            ["lone element"],
            ("empty tuple",)
        )

        for p in jparams:
            for v in values:
                expected = p + '=' + ulp.quote(json.dumps(v))
                print("Expected", expected)
                self._assert_vopteq(expected, p, v)

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              "blah", p, object())


    def test_encode_to_jarray(self):
        jparams = ('keys',) #add more here
        values = (
            ['foo', 'bar'],
            ['foo'])

        badvalues = (True,
                     False,
                     {"foo":"bar"},
                     1,
                     "string")

        for p in jparams:
            for v in values:

                print(v)
                expected = p + '=' + ulp.quote(json.dumps(v))
                self._assert_vopteq(expected, p, v)

            for v in badvalues:
                self.assertRaises(ArgumentError,
                                  self._assert_vopteq,
                                  "blah", p, v)


    def test_passthrough(self):
        values = (
            "blah",
            -1,
            "-invalid/uri&char")

        for p in _HANDLER_MAP.keys():
            for v in values:
                expected = "{0}={1}".format(p, v)
                got = make_options_string({p:v}, passthrough=True)
                self.assertEqual(expected, got)


        # Ensure we still can't use unrecognized params
        self.assertRaises(ArgumentError,
                          make_options_string,
                          {'foo':'bar'},
                          passthrough=True)


        # ensure we still can't use "stupid" params
        badvals = (object(), None, True, False)
        for bv in badvals:
            self.assertRaises(ArgumentError,
                              make_options_string,
                              {'stale':bv},
                              passthrough=True)


    def test_unrecognized(self):
        keys = ("new_param", "another_param")
        values = ("blah", -1, "-invalid-uri-char^&")
        for p in keys:
            for v in values:
                got = make_options_string({p:v},
                    unrecognized_ok=True)
                expected = "{0}={1}".format(p, v)
                self.assertEqual(expected, got)


        badvals = (object(), True, False, None)
        for bv in badvals:
            self.assertRaises(ArgumentError,
                              make_options_string,
                              {'foo':bv},
                              unrecognized_ok=True)

    def test_string_params(self):
        # This test is mainly to see that 'stupid' things don't make
        # their way through as strings, like booleans and None
        sparams = ('endkey_docid',
                   'startkey_docid')

        goodvals = ("string", -1, "OHAI!", '&&escape_me_nao&&')
        badvals = (True, False, None, object(), [])

        for p in sparams:
            for v in goodvals:
                expected = "{0}={1}".format(p, ulp.quote(str(v)))
                self._assert_vopteq(expected, p, v)

            for v in badvals:
                self.assertRaises(ArgumentError,
                                  make_options_string,
                                  {p:v})


    def test_ranges(self):
        expected = "startkey={0}".format(ulp.quote(json.dumps("foo")))
        self._assert_vopteq(expected, "mapkey_range", ["foo"])
        self._assert_vopteq_multi(
            {'startkey' : json.dumps("foo"),
             'endkey' : json.dumps("bar") },
            "mapkey_range",
            ["foo", "bar"])


        expected = "startkey_docid=bar"
        self._assert_vopteq(expected, "dockey_range", ["bar"])
        self._assert_vopteq_multi(
            {'startkey_docid' : "range_begin",
             'endkey_docid' : "range_end"},
            "dockey_range",
            ["range_begin", "range_end"])

        for p in ('mapkey_range', 'dockey_range'):
            self._assert_vopteq('', p, [])
            self._assert_vopteq('', p, UNSPEC)
            self._assert_vopteq('', p, [UNSPEC,UNSPEC])
            self._assert_vopteq('', p, [UNSPEC])

            self.assertRaises(ArgumentError,
                  self._assert_vopteq,
                  "blah", p, [object()])

            self.assertRaises(ArgumentError,
                              self._assert_vopteq,
                              "blah", p, None)
