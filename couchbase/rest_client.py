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

import base64
try:
    import json
except:
    import simplejson as json
import urllib
import socket
import time
import logger
import warnings

import requests
import httplib2

import client
from exception import ServerAlreadyJoinedException,\
    ServerUnavailableException, InvalidArgumentException,\
    BucketCreationException, ServerJoinException, BucketUnavailableException

log = logger.logger("rest_client")


#helper library methods built on top of RestConnection interface
class RestHelper(object):
    def __init__(self, rest_connection):
        self.rest = rest_connection

    def is_ns_server_running(self, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            try:
                if self.is_cluster_healthy():
                    return True
            except ServerUnavailableException:
                time.sleep(1)
        msg = 'unable to connect to the node %s even after waiting %s seconds'
        log.info(msg % (self.rest.ip, timeout_in_seconds))
        return False

    def is_cluster_healthy(self):
        #get the nodes and verify that all the nodes.status are healthy
        nodes = self.rest.node_statuses()
        return all(node.status == 'healthy' for node in nodes)

    def rebalance_reached(self, percentage=100):
        start = time.time()
        progress = 0
        retry = 0
        while progress != -1 and progress <= percentage and retry < 20:
            #-1 is error , -100 means could not retrieve progress
            progress = self.rest._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in"
                          " 2 seconds")
                retry += 1
            else:
                retry = 0
            time.sleep(.1)
        if progress < 0:
            log.error("rebalance progress code : %s" % (progress))
            return False
        else:
            duration = time.time() - start
            log.info('rebalance reached >%s percent in %s seconds ' %
                     (progress, duration))
            return True

    def is_cluster_rebalanced(self):
        #get the nodes and verify that all the nodes.status are healthy
        return self.rest.rebalance_statuses()

    #this method will rebalance the cluster by passing the remote_node as
    #ejected node
    def remove_nodes(self, knownNodes, ejectedNodes):
        self.rest.rebalance(knownNodes, ejectedNodes)
        return self.rest.monitorRebalance()

    def vbucket_map_ready(self, bucket, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            vBuckets = self.rest.get_vbuckets(bucket)
            if vBuckets:
                return True
            else:
                time.sleep(0.5)
        msg = 'vbucket map is not ready for bucket %s after waiting %s seconds'
        log.info(msg % (bucket, timeout_in_seconds))
        return False

    def bucket_exists(self, bucket):
        try:
            buckets = self.rest.get_buckets()
            names = [item.name for item in buckets]
            log.info("existing buckets : %s" % (names))
            for item in buckets:
                if item.name == bucket:
                    log.info("found bucket %s" % (bucket))
                    return True
            return False
        except Exception:
            return False

    def wait_for_node_status(self, node, expected_status, timeout_in_seconds):
        status_reached = False
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time and not status_reached:
            nodes = self.rest.node_statuses()
            for n in nodes:
                if node.id == n.id:
                    log.info('node %s status : %s' % (node.id, n.status))
                    if n.status.lower() == expected_status.lower():
                        status_reached = True
                    break
            if not status_reached:
                log.info("sleep for 5 seconds before reading the node.status"
                         " again")
                time.sleep(5)
        log.info('node %s status_reached : %s' % (node.id, status_reached))
        return status_reached

    def wait_for_replication(self, timeout_in_seconds=120):
        wait_count = 0
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            if self.all_nodes_replicated():
                break
            wait_count += 1
            if wait_count == 10:
                log.info('replication state : %s' %
                         (self.all_nodes_replicated(debug=True)))
                wait_count = 0
            time.sleep(5)
        log.info('replication state : %s' % (self.all_nodes_replicated()))
        return self.all_nodes_replicated()

    def all_nodes_replicated(self, debug=False):
        replicated = True
        nodes = self.rest.node_statuses()
        for node in nodes:
            if debug:
                log.info("node %s replication state : %s" %
                         (node.id, node.replication))
            if node.replication != 1.0:
                replicated = False
        return replicated


class RestConnection(object):
    def __init__(self, serverInfo):
        #serverInfo can be a json object
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            self.couch_api_base = serverInfo.get("couchApiBase")
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
            self.couch_api_base = None

        self.baseUrl = "http://%s:%s/" % (self.ip, self.port)
        # if couchApiBase is not set earlier, let's look it up
        if self.couch_api_base is None:
            server_config_uri = "http://%s:%s/pools/default" % (self.ip,
                                                                self.port)
            config = requests.get(server_config_uri).json
            #couchApiBase is not in node config before Couchbase Server 2.0
            self.couch_api_base = config["nodes"][0].get("couchApiBase")

    def create_design_doc(self, bucket, design_doc, function):
        api = self.couch_api_base + '%s/_design/%s' % (bucket, design_doc)
        #check if this view exists and update the rev

        headers = self._create_capi_headers()
        status, content = self._http_request(api, 'PUT', function,
                                             headers=headers)

        json_parsed = json.loads(content)

        if not status:
            raise Exception("unable to create design doc")

        return json_parsed

    def get_design_doc(self, bucket, design_doc):
        api = self.couch_api_base + '%s/_design/%s' % (bucket, design_doc)

        headers = self._create_capi_headers()
        status, content = self._http_request(api, headers=headers)

        json_parsed = json.loads(content)

        if not status:
            raise Exception("unable to get design doc")

        return json_parsed

    def delete_design_doc(self, bucket, design_doc):
        api = self.couch_api_base + '%s/_design/%s' % (bucket, design_doc)
        design_doc = self.get_design_doc(bucket, design_doc)
        if "error" in design_doc:
            raise Exception(design_doc["error"] + " because "
                            + design_doc["reason"])
        else:
            rev = design_doc["_rev"]
            #pass in the rev
            api = api + "?rev=%s" % (rev)

            headers = self._create_capi_headers()
            status, content = self._http_request(api, 'DELETE',
                                                 headers=headers)

            json_parsed = json.loads(content)
            if not status:

                raise Exception("unable to delete the design doc")

            return json_parsed

    def get_view(self, bucket, design_doc, view):
        warnings.warn("get_view is deprecated; use view_results instead",
                      DeprecationWarning)
        return self.view_results(bucket, design_doc, view, {})

    def view_results(self, bucket, design_doc, view, params, limit=100):
        view_query = '%s/_design/%s/_view/%s' % (bucket, design_doc, view)
        api = self.couch_api_base + view_query
        num_params = 0
        if limit != None:
            num_params = 1
            api += "?limit=%s" % (limit)
        for param in params:
            if num_params > 0:
                api += "&"
            else:
                api += "?"
            num_params += 1
            if param in ["key", "start_key", "end_key",
                         "startkey_docid", "endkey_docid"] or \
                         params[param] is True or \
                         params[param] is False:
                api += "%s=%s" % (param, json.dumps(params[param]))
            else:
                api += "%s=%s" % (param, params[param])

        headers = self._create_capi_headers()
        status, content = self._http_request(api, headers=headers)

        json_parsed = json.loads(content)

        if not status:
            raise Exception("unable to obtain view results for " + api + "\n"
                            + repr(status) + "\n" + content)

        return json_parsed

    def _create_capi_headers(self):
        return {'Content-Type': 'application/json',
                'Accept': '*/*'}

    #authorization must be a base64 string of username:password
    def _create_headers(self):
        if self.username == "default":
            return {'Content-Type': 'application/json', 'Accept': '*/*'}
        else:
            authorization = base64.encodestring('%s:%s' % (self.username,
                                                self.password))
            return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _http_request(self, api, method='GET', params='', headers=None,
                      timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http().request(api, method,
                                                            params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content
                else:
                    try:
                        json_parsed = json.loads(content)
                    except:
                        json_parsed = {}
                    reason = "unknown"
                    status = False
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                        status = reason
                    elif "errors" in json_parsed:
                        errors = [error for _, error in
                                  json_parsed["errors"].iteritems()]
                        reason = ", ".join(errors)
                        status = reason
                    log.error('%s error %s reason: %s %s' %
                              (api, response['status'], reason, content))
                    return status, content
            except socket.error, e:
                log.error(e)
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except httplib2.ServerNotFoundError, e:
                log.error(e)
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            time.sleep(1)

    #params serverIp : the server to add to this cluster
    #raises exceptions when
    #unauthorized user
    #server unreachable
    #can't add the node to itself ( TODO )
    #server already added
    #returns otpNode
    def add_node(self, user='', password='', remoteIp='', port='8091'):
        otpNode = None
        log.info('adding remote node : %s to this cluster @ : %s'\
        % (remoteIp, self.ip))
        api = self.baseUrl + 'controller/addNode'
        params = urllib.urlencode({'hostname': "%s:%s" % (remoteIp, port),
                                   'user': user,
                                   'password': password})

        status, content = self._http_request(api, 'POST', params)

        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
            otpNode = OtpNode(otpNodeId)
            if otpNode.ip == '127.0.0.1':
                otpNode.ip = self.ip
        else:
            if content.find('Prepare join failed. Node is already part of'
                            ' cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=remoteIp)
            elif content.find('Prepare join failed. Joining node to itself is'
                              ' not allowed') >= 0:
                raise ServerJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)
            else:
                log.error('add_node error : %s' % (content))
                raise ServerJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)

        return otpNode

    def eject_node(self, user='', password='', otpNode=None):
        if not otpNode:
            log.error('otpNode parameter required')
            return False

        api = self.baseUrl + 'controller/ejectNode'
        params = urllib.urlencode({'otpNode': otpNode,
                                   'user': user,
                                   'password': password})

        status, content = self._http_request(api, 'POST', params)

        if status:
            log.info('ejectNode successful')
        else:
            if content.find('Prepare join failed. Node is already part of'
                            ' cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=otpNode)
            else:
                # todo : raise an exception here
                log.error('eject_node error %s' % (content))
        return True

    def fail_over(self, otpNode=None):
        if not otpNode:
            log.error('otpNode parameter required')
            return False

        api = self.baseUrl + 'controller/failOver'
        params = urllib.urlencode({'otpNode': otpNode})

        status, content = self._http_request(api, 'POST', params)

        if status:
            log.info('fail_over successful')
        else:
            log.error('fail_over error : %s' % (content))

        if not status:
            return False
        return status

    def rebalance(self, otpNodes, ejectedNodes):
        knownNodes = ''
        index = 0
        for node in otpNodes:
            if not index:
                knownNodes += node
            else:
                knownNodes += ',' + node
            index += 1
        ejectedNodesString = ''
        index = 0
        for node in ejectedNodes:
            if not index:
                ejectedNodesString += node
            else:
                ejectedNodesString += ',' + node
            index += 1

        params = urllib.urlencode({'knownNodes': knownNodes,
                                   'ejectedNodes': ejectedNodesString,
                                   'user': self.username,
                                   'password': self.password})
        log.info('rebalanace params : %s' % (params))

        api = self.baseUrl + "controller/rebalance"

        status, content = self._http_request(api, 'POST', params)

        if status:
            log.info('rebalance operation started')
        else:
            log.error('rebalance operation failed')
            #extract the error
            raise InvalidArgumentException('controller/rebalance',
                                           parameters=params)

        if not status:
            return False
        return status

    def monitorRebalance(self):
        start = time.time()
        progress = 0
        retry = 0
        while progress != -1 and progress != 100 and retry < 20:
            #-1 is error , -100 means could not retrieve progress
            progress = self._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in"
                          "2 seconds")
                retry += 1
            else:
                retry = 0
            #sleep for 2 seconds
            time.sleep(2)
        if progress < 0:
            log.error("rebalance progress code : %s" % (progress))
            return False
        else:
            duration = time.time() - start
            log.info('rebalance progress took %s seconds ' % (duration))
            log.info("sleep for 10 seconds after rebalance...")
            time.sleep(10)
            return True

    def _rebalance_progress(self):
        percentage = -1
        api = self.baseUrl + "pools/default/rebalanceProgress"

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            if "status" in json_parsed:
                if "errorMessage" in json_parsed:
                    log.error('%s - rebalance failed' % (json_parsed))
                elif json_parsed["status"] == "running":
                    for key in json_parsed:
                        if key.find('@') >= 0:
                            ns_1_dictionary = json_parsed[key]
                            percentage = ns_1_dictionary['progress'] * 100
                            log.info('rebalance percentage : %s percent' %
                                     (percentage))
                            break
                    if percentage == -1:
                        percentage = 0
                else:
                    percentage = 100
        else:
            percentage = -100

        return percentage

    #if status is none , is there an errorMessage
    #convoluted logic which figures out if the rebalance failed or suceeded
    def rebalance_statuses(self):
        rebalanced = None
        api = self.baseUrl + 'pools/rebalanceStatuses'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            rebalanced = json_parsed['balanced']

        return rebalanced

    def log_client_error(self, post):
        api = self.baseUrl + 'logClientError'

        status, content = self._http_request(api, 'POST', post)

        if not status:
            log.error('unable to logClientError')

    #returns node data for this host
    def get_nodes_self(self):
        node = None
        api = self.baseUrl + 'nodes/self'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            node = RestParser().parse_get_nodes_response(json_parsed)

        return node

    def node_statuses(self):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            for key in json_parsed:
                #each key contain node info
                value = json_parsed[key]
                #get otp,get status
                node = OtpNode(id=value['otpNode'],
                               status=value['status'])
                if node.ip == '127.0.0.1':
                    node.ip = self.ip
                node.port = int(key[key.rfind(":") + 1:])
                node.replication = value['replication']
                nodes.append(node)

        return nodes

    def cluster_status(self):
        parsed = {}
        api = self.baseUrl + 'pools/default'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            parsed = json_parsed

        return parsed

    def get_pools_info(self):
        parsed = {}
        api = self.baseUrl + 'pools'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            parsed = json_parsed

        return parsed

    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            impl_version = json_parsed['implementationVersion']
            comp_version = json_parsed['componentsVersion']
            version = CouchbaseServerVersion(impl_version, comp_version)

        return version

    def get_buckets(self):
        #get all the buckets
        buckets = []
        api = '%s%s' % (self.baseUrl, 'pools/default/buckets/')

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            for item in json_parsed:
                bucketInfo = RestParser().parse_get_bucket_json(item)
                buckets.append(bucketInfo)

        return buckets

    def get_bucket_stats_for_node(self, bucket='default', node_ip=None):
        if not Node:
            log.error('node_ip not specified')
            return None

        stats = {}
        api = "%s%s%s%s%s%s" % (self.baseUrl, 'pools/default/buckets/',
                                bucket, "/nodes/", node_ip, ":8091/stats")

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                stats[stat_name] = samples[stat_name][0]

        return stats

    def get_nodes(self):
        nodes = []
        api = self.baseUrl + 'pools/default'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            if "nodes" in json_parsed:
                for json_node in json_parsed["nodes"]:
                    node = RestParser().parse_get_nodes_response(json_node)
                    node.rest_username = self.username
                    node.rest_password = self.password
                    node.port = self.port
                    if node.ip == "127.0.0.1":
                        node.ip = self.ip
                    nodes.append(node)

        return nodes

    def get_bucket_stats(self, bucket='default'):
        stats = {}
        api = "".join([self.baseUrl, 'pools/default/buckets/', bucket,
                      "/stats"])

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if samples[stat_name]:
                    last_sample = len(samples[stat_name]) - 1
                    if last_sample:
                        stats[stat_name] = samples[stat_name][last_sample]

        return stats

    def get_bucket(self, bucket='default'):
        bucketInfo = None
        api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket)
        status, content = self._http_request(api)

        if status:
            bucketInfo = RestParser().parse_get_bucket_response(content)
            # log.debug('set stats to %s' % (bucketInfo.stats.ram))
        else:
            raise BucketUnavailableException(ip=self.ip, bucket_name=bucket,
                                             error=status)

        return bucketInfo

    def get_vbuckets(self, bucket='default'):
        return self.get_bucket(bucket).vbuckets

    def delete_bucket(self, bucket='default'):
        api = '%s%s%s' % (self.baseUrl, '/pools/default/buckets/', bucket)

        status, content = self._http_request(api, 'DELETE')
        if not status:
            return False
        return status

    # figure out the proxy port
    def create_bucket(self, bucket='',
                      ramQuotaMB=1,
                      authType='none',
                      saslPassword='',
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase'):
        api = '%s%s' % (self.baseUrl, '/pools/default/buckets')
        params = urllib.urlencode({})
        #this only works for default bucket ?
        if bucket == 'default':
            params = urllib.urlencode({'name': bucket,
                                       'authType': 'sasl',
                                       'saslPassword': saslPassword,
                                       'ramQuotaMB': ramQuotaMB,
                                       'replicaNumber': replicaNumber,
                                       'proxyPort': proxyPort,
                                       'bucketType': bucketType})

        elif authType == 'none':
            params = urllib.urlencode({'name': bucket,
                                       'ramQuotaMB': ramQuotaMB,
                                       'authType': authType,
                                       'replicaNumber': replicaNumber,
                                       'proxyPort': proxyPort,
                                       'bucketType': bucketType})

        elif authType == 'sasl':
            params = urllib.urlencode({'name': bucket,
                                       'ramQuotaMB': ramQuotaMB,
                                       'authType': authType,
                                       'saslPassword': saslPassword,
                                       'replicaNumber': replicaNumber,
                                       'proxyPort': self.get_nodes_self().moxi,
                                       'bucketType': bucketType})

        log.info("%s with param: %s" % (api, params))

        status, content = self._http_request(api, 'POST', params)

        if not status:
            raise BucketCreationException(ip=self.ip, bucket_name=bucket,
                                          error=status)

        return status

    #return AutoFailoverSettings
    def get_autofailover_settings(self):
        settings = None
        api = self.baseUrl + 'settings/autoFailover'

        status, content = self._http_request(api)

        json_parsed = json.loads(content)

        if status:
            settings = AutoFailoverSettings()
            settings.enabled = json_parsed["enabled"]
            settings.count = json_parsed["count"]
            settings.timeout = json_parsed["timeout"]

        return settings

    def update_autofailover_settings(self, enabled, timeout, max_nodes):
        if enabled:
            params = urllib.urlencode({'enabled': 'true',
                                       'timeout': timeout,
                                       'maxNodes': max_nodes})
        else:
            params = urllib.urlencode({'enabled': 'false',
                                       'timeout': timeout,
                                       'maxNodes': max_nodes})
        api = self.baseUrl + 'settings/autoFailover'
        log.info('settings/autoFailover params : %s' % (params))

        status, content = self._http_request(api, 'POST', params)
        if not status:
            return False
        return status

    def reset_autofailover(self):
        api = self.baseUrl + 'settings/autoFailover/resetCount'

        status, content = self._http_request(api, 'POST', '')
        if not status:
            return False
        return status

    def enable_autofailover_alerts(self, recipients, sender, email_username,
                                   email_password, email_host='localhost',
                                   email_port=25, email_encrypt='false',
                                   alerts=('auto_failover_node,'
                                           'auto_failover_maximum_reached')):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.urlencode({'enabled': 'true',
                                   'recipients': recipients,
                                   'sender': sender,
                                   'emailUser': email_username,
                                   'emailPass': email_password,
                                   'emailHost': email_host,
                                   'emailPrt': email_port,
                                   'emailEncrypt': email_encrypt,
                                   'alerts': alerts})
        log.info('settings/alerts params : %s' % (params))

        status, content = self._http_request(api, 'POST', params)
        if not status:
            return False
        return status

    def disable_autofailover_alerts(self):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.urlencode({'enabled': 'false'})
        log.info('settings/alerts params : %s' % (params))

        status, content = self._http_request(api, 'POST', params)
        if not status:
            return False
        return status

    def stop_rebalance(self):
        api = self.baseUrl + '/controller/stopRebalance'

        status, content = self._http_request(api, 'POST')
        if not status:
            return False
        return status

    def set_data_path(self, data_path):
        api = self.baseUrl + '/nodes/self/controller/settings'
        params = urllib.urlencode({'path': data_path})
        log.info('/nodes/self/controller/settings params : %s' % (params))

        status, content = self._http_request(api, 'POST', params)
        if not status:
            return False
        return status


class CouchbaseServerVersion:
    def __init__(self, implementationVersion='', componentsVersion=''):
        self.implementationVersion = implementationVersion
        self.componentsVersion = componentsVersion


#this class will also contain more node related info
class OtpNode(object):
    def __init__(self, id='', status=''):
        self.id = id
        self.ip = ''
        self.replication = ''
        self.port = 8091
        #extract ns ip from the otpNode string
        #its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
        self.status = status


class NodeInfo(object):
    def __init__(self):
        self.availableStorage = None  # list
        self.memoryQuota = None


class NodeDataStorage(object):
    def __init__(self):
        self.type = ''  # hdd or ssd
        self.path = ''
        self.quotaMb = ''
        self.state = ''  # ok

    def __str__(self):
        return '%s' % ({'type': self.type,
                             'path': self.path,
                             'quotaMb': self.quotaMb,
                             'state': self.state})


class NodeDiskStorage(object):
    def __init__(self):
        self.type = 0
        self.path = ''
        self.sizeKBytes = 0
        self.usagePercent = 0


class Bucket(object):
    def __init__(self):
        self.name = ''
        self.port = 11211
        self.type = ''
        self.nodes = None
        self.stats = None
        self.servers = []
        self.vbuckets = []
        self.forward_map = []
        self.numReplicas = 0
        self.saslPassword = ""
        self.authType = ""


class Node(object):
    def __init__(self):
        self.uptime = 0
        self.memoryTotal = 0
        self.memoryFree = 0
        self.mcdMemoryReserved = 0
        self.mcdMemoryAllocated = 0
        self.status = ""
        self.hostname = ""
        self.clusterCompatibility = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.moxi = 11211
        self.memcached = 11210
        self.id = ""
        self.ip = ""
        self.rest_username = ""
        self.rest_password = ""


class AutoFailoverSettings(object):
    def __init__(self):
        self.enabled = True
        self.timeout = 0
        self.count = 0


class NodePort(object):
    def __init__(self):
        self.proxy = 0
        self.direct = 0


class BucketStats(object):
    def __init__(self):
        self.quotaPercentUsed = 0
        self.opsPerSec = 0
        self.diskFetches = 0
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0


class vBucket(object):
    def __init__(self):
        self.master = ''
        self.replica = []
        self.id = -1


class RestParser(object):
    def parse_get_nodes_response(self, parsed):
        node = Node()
        node.uptime = parsed['uptime']
        node.memoryFree = parsed['memoryFree']
        node.memoryTotal = parsed['memoryTotal']
        node.mcdMemoryAllocated = parsed['mcdMemoryAllocated']
        node.mcdMemoryReserved = parsed['mcdMemoryReserved']
        node.status = parsed['status']
        node.hostname = parsed['hostname']
        node.clusterCompatibility = parsed['clusterCompatibility']
        node.version = parsed['version']
        node.os = parsed['os']
        if "otpNode" in parsed:
            node.id = parsed["otpNode"]
            if parsed["otpNode"].find('@') >= 0:
                node.ip = node.id[node.id.index('@') + 1:]

        # memoryQuota
        if 'memoryQuota' in parsed:
            node.memoryQuota = parsed['memoryQuota']
        if 'availableStorage' in parsed:
            availableStorage = parsed['availableStorage']
            for key in availableStorage:
                #let's assume there is only one disk in each noce
                dict_parsed = parsed['availableStorage']
                if 'path' in dict_parsed and 'sizeKBytes' in dict_parsed and\
                    'usagePercent' in dict_parsed:
                    diskStorage = NodeDiskStorage()
                    diskStorage.path = dict_parsed['path']
                    diskStorage.sizeKBytes = dict_parsed['sizeKBytes']
                    diskStorage.type = key
                    diskStorage.usagePercent = dict_parsed['usagePercent']
                    node.availableStorage.append(diskStorage)
                    log.info(diskStorage)

        if 'storage' in parsed:
            storage = parsed['storage']
            for key in storage:
                disk_storage_list = storage[key]
                for dict_parsed in disk_storage_list:
                    if 'path' in dict_parsed and 'state' in dict_parsed and\
                        'quotaMb' in dict_parsed:
                        dataStorage = NodeDataStorage()
                        dataStorage.path = dict_parsed['path']
                        dataStorage.quotaMb = dict_parsed['quotaMb']
                        dataStorage.state = dict_parsed['state']
                        dataStorage.type = key
                        node.storage.append(dataStorage)

        # ports":{"proxy":11211,"direct":11210}
        if "ports" in parsed:
            ports = parsed["ports"]
            if "proxy" in ports:
                node.moxi = ports["proxy"]
            if "direct" in ports:
                node.memcached = ports["direct"]
        return node

    def parse_get_bucket_response(self, response):
        parsed = json.loads(response)
        return self.parse_get_bucket_json(parsed)

    def parse_get_bucket_json(self, parsed):
        bucket = Bucket()
        bucket.name = parsed['name']
        bucket.type = parsed['bucketType']
        bucket.port = parsed['proxyPort']
        bucket.authType = parsed["authType"]
        bucket.saslPassword = parsed["saslPassword"]
        bucket.nodes = list()
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            bucket.servers.extend(serverList)
            if "numReplicas" in vBucketServerMap:
                bucket.numReplicas = vBucketServerMap["numReplicas"]
            #vBucketMapForward
            if 'vBucketMapForward' in vBucketServerMap:
                #let's gather the forward map
                vBucketMapForward = vBucketServerMap['vBucketMapForward']
                for vbucket in vBucketMapForward:
                    #there will be n number of replicas
                    vbucketInfo = vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                (vbucketInfo.replica
                                 .append(serverList[vbucket[i]]))
                    bucket.forward_map.append(vbucketInfo)
            vBucketMap = vBucketServerMap['vBucketMap']
            counter = 0
            for vbucket in vBucketMap:
                #there will be n number of replicas
                vbucketInfo = vBucket()
                vbucketInfo.master = serverList[vbucket[0]]
                if vbucket:
                    for i in range(1, len(vbucket)):
                        if vbucket[i] != -1:
                            vbucketInfo.replica.append(serverList[vbucket[i]])
                vbucketInfo.id = counter
                counter += 1
                bucket.vbuckets.append(vbucketInfo)
                #now go through each vbucket and populate the info
            #who is master , who is replica
        # get the 'storageTotals'
        log.debug('read %s vbuckets' % (len(bucket.vbuckets)))
        stats = parsed['basicStats']
        #vBucketServerMap
        bucketStats = BucketStats()
        log.debug('stats:%s' % (stats))
        bucketStats.quotaPercentUsed = stats['quotaPercentUsed']
        bucketStats.opsPerSec = stats['opsPerSec']
        if 'diskFetches' in stats:
            bucketStats.diskFetches = stats['diskFetches']
        bucketStats.itemCount = stats['itemCount']
        bucketStats.diskUsed = stats['diskUsed']
        bucketStats.memUsed = stats['memUsed']
        quota = parsed['quota']
        bucketStats.ram = quota['ram']
        bucket.stats = bucketStats
        nodes = parsed['nodes']
        for nodeDictionary in nodes:
            node = Node()
            node.uptime = nodeDictionary['uptime']
            node.memoryFree = nodeDictionary['memoryFree']
            node.memoryTotal = nodeDictionary['memoryTotal']
            node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            node.status = nodeDictionary['status']
            node.hostname = nodeDictionary['hostname']
            cluster_compat = 'clusterCompatibility'
            if cluster_compat in nodeDictionary:
                node.clusterCompatibility = nodeDictionary[cluster_compat]
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "proxy" in ports:
                    node.moxi = ports["proxy"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket
