#!/usr/bin/env python

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


# This file demonstrates some of the functionalities available with
# view queries. This creates a bunch of key-value pairs where the value is
# a number. It also creates a view to index the key-value pairs by the
# number itself, and finally queries the view to return the ten items with
# the highest values.

from argparse import ArgumentParser
import random
import pprint

from couchbase.bucket import Bucket

ap = ArgumentParser()

ap.add_argument('-D', '--create-design', default=False,
                action='store_true',
                help='whether to create the design')

ap.add_argument('-n', '--number-of-terms', default=10,
                type=int, help="How many terms to generate")

options = ap.parse_args()

c = Bucket('couchbase://localhost/default')

DESIGN = {
    '_id': '_design/search_keywords',
    'language': 'javascript',
    'views': {
        'top_keywords': {
            'map':
            """
            function(doc) {
                if (typeof doc === 'number') {
                    emit(doc, null);
                }
            }
            """
        }
    }
}

if options.create_design:
    bm = c.bucket_manager()
    bm.design_create('search_keywords', DESIGN, use_devmode=False, syncwait=5)

NOUNS = ['cow', 'cat', 'dog', 'computer', 'WMD']
ADJECTIVES = ['happy', 'sad', 'thoughtful', 'extroverted']

kv = {}

for x in range(options.number_of_terms):
    n = random.choice(NOUNS)
    a = random.choice(ADJECTIVES)
    kv[" ".join([a, n])] = random.randint(1, 100000)

c.upsert_multi(kv)

vret = c.query('search_keywords',
               'top_keywords',
               limit=10,
               descending=True)

for row in vret:
    pprint.pprint(row, indent=4)

# Sample output:
#[   {   u'id': u'WMD sad', u'key': 92772, u'value': None},
#    {   u'id': u'WMD thoughtful', u'key': 76222, u'value': None},
#    {   u'id': u'cow happy', u'key': 71984, u'value': None},
#    {   u'id': u'computer sad', u'key': 68849, u'value': None},
#    {   u'id': u'cat thoughtful', u'key': 68417, u'value': None},
#    {   u'id': u'computer thoughtful', u'key': 67518, u'value': None},
#    {   u'id': u'dog thoughtful', u'key': 67350, u'value': None},
#    {   u'id': u'computer extroverted', u'key': 63279, u'value': None},
#    {   u'id': u'cow thoughtful', u'key': 60962, u'value': None},
#    {   u'id': u'cow sad', u'key': 49510, u'value': None}]
