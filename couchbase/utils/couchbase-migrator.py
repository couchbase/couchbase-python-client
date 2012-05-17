#!/usr/bin/env python

#
# Copyright 2012, Couchbase, Inc.
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

import sys
import getopt

import couchbase.migrator

#sources: json, csv, sqlite, couchdb, couchbase, membase, memcached, membase-sqlite
#dest: json (chunked), couchbase


def usage(err=None):
    if err:
        print "Error: %s\n" % err
        r = 1
    else:
        r = 0

    print """\
Syntax: couchbase-migrator [options]

Options:
 -h, --help

 -s <source>, --source=<source>
     Data source that will be imported from

 -d <destination>, --destination=<destination>
     Data destination that will be exported to
"""
    """
 -o, --overwrite
     Overwrite any items that are in the destination

 --dry-run
     Do not write any data, just show what would be written

 -v, --verbose

 -q, --quiet

"""
    print "Sources:"
    for source in couchbase.migrator.sources:
        print " " + source['type']
        print "     " + source['example']
        print

    print

    print "Destinations:"
    for destination in couchbase.migrator.destinations:
        print " " + destination['type']
        print "     " + destination['example']
        print

    sys.exit(r)

class Config(object):
    def __init__(self):
        self.source = ''
        self.destination = ''
        self.overwrite = False
        self.dryrun = False
        self.verbose = False
        self.quiet = False

def parse_args(argv):
    config = Config()
    try:
        opts, args = getopt.getopt(argv[1:],
                                     'hs:d:ovq', [
                'help',
                'source=',
                'destination=',
                'overwrite',
                'dry-run',
                'verbose',
                'quiet',
                ])
        for o, a in opts:
            if o == '-h' or o == '--help':
                usage()
            elif o == '-s' or o == '--source':
                config.source = a
            elif o == '-d' or o == '--destination':
                config.destination = a
            elif o == '-o' or o == '--overwrite':
                config.overwrite = True
            elif o == '--dry-run':
                config.dryrun = True
            elif o == '-v' or o == '--verbose':
                config.verbose = True
            elif o == '-q' or o == '--quiet':
                config.quiet = True

        msg = ""
        if not config.source or not config.destination:
            usage("missing source or destination")

    except IndexError:
        usage()
    except getopt.GetoptError, err:
        usage(err)

    return config


if __name__ == "__main__":
    config = parse_args(sys.argv)

    count = 0

    reader = couchbase.migrator.reader(config.source)
    writer = couchbase.migrator.writer(config.destination)
    for record in reader:
        writer.write(record)
        count += 1

    reader.close()
    writer.close()

    print 'migrated %s items' % (count)
