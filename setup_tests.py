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

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser
import getopt
import json
import os
import sys
try:
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import HTTPError
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2


CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'tests.ini')


def usage():
    print("usage: \n -s <set the memory quota of the node (in MB)\n")


def parse_options(options):
    memquota = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 's:')
    except getopt.GetoptError as e:
        print(e)
        usage()
        sys.exit()
    data_base_path = os.getcwd() + '/data'
    for o, value in opts:
        if o == '-s':
            memquota = value
        else:
            usage()
            sys.exit()
    return memquota

def server_settings(config_file):
    config = ConfigParser()
    config.read(config_file)

    return {
        'host': config.get('realserver', 'host'),
        'port': config.getint('realserver', 'port'),
        'username': config.get('realserver', 'admin_username'),
        'password': config.get('realserver', 'admin_password'),
        'bucket_prefix': config.get('realserver', 'bucket_prefix'),
        'bucket_password': config.get('realserver', 'bucket_password'),
        'bucket_port': 11400
    }


def set_mem_quota(opener, mem_quota):
    url = 'http://localhost:8091/pools/default'
    params =  'memoryQuota={0}'.format(mem_quota)
    opener.open(url, params.encode('utf-8'))
    print("Memory quota set to {0} MB".format(mem_quota))

def delete_bucket(opener, settings, bucket_name):
    urllib2.install_opener(opener)
    url = 'http://{0}:{1}/pools/default/buckets/{2}'.format(
        settings['host'], settings['port'], bucket_name)
    req = urllib2.Request(url)
    req.get_method = lambda: 'DELETE'
    try:
        urllib2.urlopen(req)
    except HTTPError as e:
        print("HTTP error {0}: {1}".format(e.code, e.reason))
        print(e.read())
    print("Deleted bucket with name '{0}'".format(bucket_name))

def create_bucket(opener, settings, bucket_name, protected=False):
    """Create a bucket

    Retry increase ports until a free one was found.
    The bucket name is supplied and not used from `setting`, so you can
    create different buckets with the same server settings.
    """
    params = 'name={0}&ramQuotaMB=100&&replicaNumber=0'.format(bucket_name)
    if protected:
        params += '&authType=sasl&saslPassword=secret'
    else:
        params += '&authType=none&proxyPort={0}'.format(
            settings['bucket_port'])
    url = 'http://{0}:{1}/pools/default/buckets'.format(settings['host'],
                                                        settings['port'])
    try:
        opener.open(url, params.encode('utf-8'))
    except HTTPError as e:
        resp = json.loads(e.read().decode('utf-8'))
        if e.code == 400 and 'proxyPort' in resp['errors']:
            print("Port {0} is already in use, trying a higher one".format(
                settings['bucket_port']))
            settings['bucket_port'] += 1
            create_bucket(opener, settings, bucket_name, protected)
            return
        elif e.code == 400 and 'name' in resp['errors']:
            print("Recreating bucket with name '{0}'".format(bucket_name))
            delete_bucket(opener, settings, bucket_name)
            create_bucket(opener, settings, bucket_name, protected)
            return
        elif e.code == 400 and 'ramQuotaMB' in resp['errors']:
            print("Error: Memory quota for this server is to small. "
                  "Use the '-s' parameter to increase it")
            sys.exit()
        else:
            print("HTTP error {0}: {1}".format(e.code, e.reason))
            print(json.dumps(resp))
            raise e
    msg = "Created bucket with name '{0}' ".format(bucket_name)
    if protected:
        msg += "with password '{0}'".format(settings['bucket_password'])
    else:
        msg += "at port {0}".format(settings['bucket_port'])
    print(msg)


class PasswordManager(urllib2.HTTPPasswordMgr):
    def __init__(self, username, password):
        self.auth = (username, password)

    def find_user_password(self, realm, authuri):
        return self.auth


def main():
    mem_quota = parse_options(sys.argv[1:])
    settings = server_settings(CONFIG_FILE)

    password_mgr = PasswordManager(settings['username'], settings['password'])
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)

    if mem_quota:
        set_mem_quota(opener, mem_quota)

    create_bucket(opener, settings, settings['bucket_prefix'])
    create_bucket(opener, settings, settings['bucket_prefix'] + '_sasl', True)


if __name__ == '__main__':
    main()
