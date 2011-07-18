#!/usr/bin/python
import random
import uuid
from membaseclient import VBucketAwareMembaseClient
from optparse import OptionParser
from util import ProgressBar, StringUtil
import sys

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node",
                      help="node's ns_server ip:port", metavar="192.168.1.1:8091")
    parser.add_option("-u", "--user", dest="username",
                      help="node username", metavar="Administrator", default="Administrator")
    parser.add_option("-p", "--password", dest="password",
                      help="node password", metavar="asdasd")
    parser.add_option("-b", "--bucket", dest="bucket", help="which bucket to insert data",
                      default="default", metavar="default")
    parser.add_option("-j", "--json", dest="load_json", help="insert json data",
                      default=False, metavar="True")
    parser.add_option("-i", "--items", dest="items", help="number of items to be inserted",
                      default=100, metavar="100")

    parser.add_option("--size", dest="value_size", help="value size,default is 256 byte",
                      default=512, metavar="100")

    parser.add_option("--prefix", dest="key_prefix",
                      help="prefix to use for memcached keys and json _ids,default is uuid string",
                      default=str(uuid.uuid4())[:5], metavar="pymc")

    options, args = parser.parse_args()

    node = options.node
    #if port is not given use :8091
    if node.find(":") == -1:
        hostname = node
        port = 8091
    else:
        hostname = node[:node.find(":")]
        port = node[node.find(":") + 1:]
    server = {"ip": hostname,
              "port": port,
              "rest_username": options.username,
              "rest_password": options.password,
              "username": options.username,
              "password": options.password}
    print server
    v = None
    try:
        v = VBucketAwareMembaseClient(server, options.bucket)
        number_of_items = int(options.items)
        bar = ProgressBar(0, number_of_items, 77)
        old_bar_string = ""
        value = StringUtil.create_value("*", options.value_size)
        for i in range(0, number_of_items):
            key = "{0}-{1}".format(options.key_prefix, str(uuid.uuid4())[:5])
            if options.load_json:
                document = "\"name\":\"pymc-{0}\"".format(key, key)
                document = document + ",\"age\":{0}".format(random.randint(0, 1000))
                document = "{" + document + "}"
                a, b, c = v.set(key, 0, 0, document)
            else:
                a, b, c = v.set(key, 0, 0, value)
            a, b, c = v.get(key)

            bar.updateAmount(i)
            if old_bar_string != str(bar):
                sys.stdout.write(str(bar) + '\r')
                sys.stdout.flush()
                old_bar_string = str(bar)

        bar.updateAmount(number_of_items)
        sys.stdout.write(str(bar) + '\r')
        sys.stdout.flush()
        v.done()
        print ""
    except:
        print ""
        if v:
            v.done()