import base64
import json
import urllib
import httplib2
import socket
import time
import logger
from exception import ServerAlreadyJoinedException, ServerUnavailableException, InvalidArgumentException
from membase.api.exception import BucketCreationException, ServerJoinException

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
        msg = 'unable to connect to the node {0} even after waiting {1} seconds'
        log.info(msg.format(self.rest.ip, timeout_in_seconds))
        return False


    def is_cluster_healthy(self):
        #get the nodes and verify that all the nodes.status are healthy
        nodes = self.rest.node_statuses()
        return all(node.status == 'healthy' for node in nodes)

    def rebalance_reached(self, percentage=100):
        start = time.time()
        progress = 0
        retry = 0
        while progress is not -1 and progress <= percentage and retry < 20:
            #-1 is error , -100 means could not retrieve progress
            progress = self.rest._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in 2 seconds")
                retry += 1
            else:
                retry = 0
                #sleep for 2 seconds
            time.sleep(0.1)
        if progress < 0:
            log.error("rebalance progress code : {0}".format(progress))
            return False
        else:
            duration = time.time() - start
            log.info('rebalance reached >{0}% in {1} seconds '.format(progress, duration))
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
        msg = 'vbucket map is not ready for bucket {0} after waiting {1} seconds'
        log.info(msg.format(bucket, timeout_in_seconds))
        return False

    def bucket_exists(self, bucket):
        try:
            buckets = self.rest.get_buckets()
            names = [item.name for item in buckets]
            log.info("existing buckets : {0}".format(names))
            for item in buckets:
                if item.name == bucket:
                    log.info("found bucket {0}".format(bucket))
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
                    log.info('node {0} status : {1}'.format(node.id, n.status))
                    if n.status.lower() == expected_status.lower():
                        status_reached = True
                    break
            if not status_reached:
                log.info("sleep for 5 seconds before reading the node.status again")
                time.sleep(5)
        log.info('node {0} status_reached : {1}'.format(node.id, status_reached))
        return status_reached

    def wait_for_replication(self, timeout_in_seconds=120):
        wait_count = 0
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            if self.all_nodes_replicated():
                break
            wait_count += 1
            if wait_count == 10:
                log.info('replication state : {0}'.format(self.all_nodes_replicated(debug=True)))
                wait_count = 0
            time.sleep(5)
        log.info('replication state : {0}'.format(self.all_nodes_replicated()))
        return self.all_nodes_replicated()

    def all_nodes_replicated(self, debug=False):
        replicated = True
        nodes = self.rest.node_statuses()
        for node in nodes:
            if debug:
                log.info("node {0} replication state : {1}".format(node.id, node.replication))
            if node.replication != 1.0:
                replicated = False
        return replicated


class RestConnection(object):
    #port is always 8091
    def __init__(self, ip, username='Administrator', password='password'):
        #throw some error here if the ip is null ?
        self.ip = ip
        self.username = username
        self.password = password
        self.baseUrl = "http://{0}:8091/".format(self.ip)
        self.port = 8091

    def __init__(self, serverInfo):
        #serverInfo can be a json object
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)


    #authorization mut be a base64 string of username:password
    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def init_cluster(self, username='Administrator', password='password'):
        api = self.baseUrl + 'settings/web'
        params = urllib.urlencode({'port': '8091',
                                   'username': username,
                                   'password': password})
        log.info('settings/web params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("settings/web response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('init_cluster error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error as socket_error:
            log.error(socket_error)
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def init_cluster_port(self, username='Administrator', password='password'):
        api = self.baseUrl + 'settings/web'
        params = urllib.urlencode({'port': '8091',
                                   'username': username,
                                   'password': password})

        log.info('settings/web params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("settings/web response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('init_cluster_port error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def init_cluster_memoryQuota(self, username='Administrator',
                                 password='password',
                                 memoryQuota=200):
        api = self.baseUrl + 'pools/default'
        print api
        params = urllib.urlencode({'memoryQuota': memoryQuota,
                                   'username': username,
                                   'password': password})
        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            if response['status'] == '400':
                log.error('init_cluster_memoryQuota error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error as se:
            print se
            print "socket error"
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            print "server error"
            raise ServerUnavailableException(ip=self.ip)

    #params serverIp : the server to add to this cluster
    #raises exceptions when
    #unauthorized user
    #server unreachable
    #can't add the node to itself ( TODO )
    #server already added
    #returns otpNode
    def add_node(self, user='', password='', remoteIp='', port='8091' ):
        otpNode = None
        log.info('adding remote node : {0} to this cluster @ : {1}'\
        .format(remoteIp, self.ip))
        api = self.baseUrl + 'controller/addNode'
        params = urllib.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password})
        try:
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            log.info('add_node response : {0} content : {1}'.format(response, content))
            if response['status'] == '400':
                log.error('error occured while adding remote node: {0}'.format(remoteIp))
                if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                    raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                       remoteIp=remoteIp)
                elif content.find('Prepare join failed. Joining node to itself is not allowed') >= 0:
                    raise ServerJoinException(nodeIp=self.ip,
                                              remoteIp=remoteIp)
                else:
                    #todo: raise an exception here
                    log.error('get_pools error : {0}'.format(content))
            elif response['status'] == '200':
                log.info('added node {0} : response {1}'.format(remoteIp, content))
                dict = json.loads(content)
                otpNodeId = dict['otpNode']
                otpNode = OtpNode(otpNodeId)
                if otpNode.ip == '127.0.0.1':
                    otpNode.ip = self.ip
            return otpNode
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


    def eject_node(self, user='', password='', otpNode=None ):
        if not otpNode:
            log.error('otpNode parameter required')
            return False
        try:
            api = self.baseUrl + 'controller/ejectNode'
            params = urllib.urlencode({'otpNode': otpNode,
                                       'user': user,
                                       'password': password})
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            if response['status'] == '400':
                if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                    raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                       remoteIp=otpNode)
                else:
                    # todo : raise an exception here
                    log.error('eject_node error {0}'.format(content))
            elif response['status'] == '200':
                log.info('ejectNode successful')
            return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def fail_over(self, otpNode=None ):
        if not otpNode:
            log.error('otpNode parameter required')
            return False
        try:
            api = self.baseUrl + 'controller/failOver'
            params = urllib.urlencode({'otpNode': otpNode})
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            log.info("failover response : {0}".format(response))
            if response['status'] == '400':
                log.error('fail_over error : {0}'.format(content))
                return False
            elif response['status'] == '200':
                log.info('fail_over successful')
                return True
            return False
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


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
        log.info('rebalanace params : {0}'.format(params))

        api = self.baseUrl + "controller/rebalance"
        try:
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            log.info('rebalance: {0}'.format(response))
            if response['status'] == '400':
                #extract the error
                raise InvalidArgumentException('controller/rebalance',
                                               parameters=params)
            elif response['status'] == '200':
                log.info('rebalance operation started')
            return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def monitorRebalance(self):
        start = time.time()
        progress = 0
        retry = 0
        while progress is not -1 and progress is not 100 and retry < 20:
            #-1 is error , -100 means could not retrieve progress
            progress = self._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in 2 seconds")
                retry += 1
            else:
                retry = 0
                #sleep for 2 seconds
            time.sleep(2)
        if progress < 0:
            log.error("rebalance progress code : {0}".format(progress))
            return False
        else:
            duration = time.time() - start
            log.info('rebalance progress took {0} seconds '.format(duration))
            log.info("sleep for 10 seconds after rebalance...")
            time.sleep(10)
            return True

    def _rebalance_progress(self):
        percentage = -1
        api = self.baseUrl + "pools/default/rebalanceProgress"
        try:
            response, content = httplib2.Http().request(api,
                                                        headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error , how ?
                log.error('unable to obtain rebalance progress ?')
                log.error(content)
                log.error(response)
                percentage = -100
            elif response['status'] == '200':
                parsed = json.loads(content)
                if parsed.has_key('status'):
                    if parsed.has_key('errorMessage'):
                        log.error('{0} - rebalance failed'.format(parsed))
                    elif parsed['status'] == 'running':
                        for key in parsed:
                            if key.find('@') >= 0:
                                ns_1_dictionary = parsed[key]
                                percentage = ns_1_dictionary['progress'] * 100
                                log.info('rebalance percentage : {0} %' .format(percentage))
                                break
                        if percentage == -1:
                            percentage = 0
                    else:
                        percentage = 100
            else:
                log.error('unable to obtain rebalance progress ?')
                log.error(content)
                log.error(response)
                percentage = -100
            if percentage == -1:
                print response, content
            return percentage
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
            #if status is none , is there an errorMessage
            #convoluted logic which figures out if the rebalance failed or suceeded

    def rebalance_statuses(self):
        api = self.baseUrl + 'pools/rebalanceStatuses'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to retrieve nodesStatuses')
            elif response['status'] == '200':
                parsed = json.loads(content)
                rebalanced = parsed['balanced']
                return rebalanced

        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def log_client_error(self, post):
        api = self.baseUrl + 'logClientError'
        try:
            response, content = httplib2.Http().request(api, 'POST', body=post, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to logClientError')
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    #retuns node data for this host
    def get_nodes_self(self):
        node = None
        api = self.baseUrl + 'nodes/self'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to retrieve nodesStatuses')
            elif response['status'] == '200':
                parsed = json.loads(content)
                node = RestParser().parse_get_nodes_response(parsed)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return node

    def node_statuses(self):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to retrieve nodesStatuses')
            elif response['status'] == '200':
                parsed = json.loads(content)
                for key in parsed:
                    #each key contain node info
                    value = parsed[key]
                    #get otp,get status
                    node = OtpNode(id=value['otpNode'],
                                   status=value['status'])
                    if node.ip == '127.0.0.1':
                        node.ip = self.ip
                    node.port = int(key[key.rfind(":") + 1:])
                    node.replication = value['replication']
                    nodes.append(node)
                    #let's also populate the membase_version_info
            return nodes
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


    def cluster_status(self):
        parsed = []
        api = self.baseUrl + 'pools/default'
        try:
            response, content = httplib2.Http().request(api, 'GET', headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to retrieve pools/default')
            elif response['status'] == '200':
                parsed = json.loads(content)
            return parsed
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_pools_info(self):
        api = self.baseUrl + 'pools'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_pools error {0}'.format(content))
            elif response['status'] == '200':
                parsed = json.loads(content)
                return parsed
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_pools error {0}'.format(content))
            elif response['status'] == '200':
                parsed = json.loads(content)
                version = MembaseServerVersion(parsed['implementationVersion'], parsed['componentsVersion'])
            return version
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_buckets(self):
        #get all the buckets
        buckets = []
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets/')
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_buckets error {0}'.format(content))
            elif response['status'] == '200':
                parsed = json.loads(content)
                # for each elements
                for item in parsed:
                    bucketInfo = RestParser().parse_get_bucket_json(item)
                    buckets.append(bucketInfo)
                return buckets
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return buckets

    def get_bucket_stats_for_node(self, bucket='default', node_ip=None):
        if not Node:
            log.error('node_ip not specified')
            return None
        api = "{0}{1}{2}{3}{4}{5}".format(self.baseUrl, 'pools/default/buckets/',
                                          bucket, "/nodes/", node_ip, ":8091/stats")
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_bucket error {0}'.format(content))
            elif response['status'] == '200':
                parsed = json.loads(content)
                #let's just return the samples
                #we can also go through the list
                #and for each item remove all items except the first one ?
                op = parsed["op"]
                samples = op["samples"]
                stats = {}
                #for each sample
                for stat_name in samples:
                    stats[stat_name] = samples[stat_name][0]
                return stats
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_nodes(self):
        nodes = []
        api = self.baseUrl + 'pools/default'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                log.error('unable to retrieve nodesStatuses')
            elif response['status'] == '200':
                parsed = json.loads(content)
                if "nodes" in parsed:
                    for json_node in parsed["nodes"]:
                        node = RestParser().parse_get_nodes_response(json_node)
                        node.rest_username = self.username
                        node.rest_password = self.password
                        node.port = self.port
                        if node.ip == "127.0.0.1":
                            node.ip = self.ip
                        nodes.append(node)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return nodes


    def get_bucket_stats(self, bucket='default'):
        api = "{0}{1}{2}{3}".format(self.baseUrl, 'pools/default/buckets/', bucket, "/stats")
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_bucket error {0}'.format(content))
            elif response['status'] == '200':
                parsed = json.loads(content)
                #let's just return the samples
                #we can also go through the list
                #and for each item remove all items except the first one ?
                op = parsed["op"]
                samples = op["samples"]
                stats = {}
                #for each sample
                for stat_name in samples:
                    if samples[stat_name]:
                        last_sample = len(samples[stat_name]) - 1
                        if last_sample:
                            stats[stat_name] = samples[stat_name][last_sample]
                return stats
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_bucket(self, bucket='default'):
        bucketInfo = None
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket)
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                log.error('get_bucket error {0}'.format(content))
            elif response['status'] == '200':
                bucketInfo = RestParser().parse_get_bucket_response(content)
                #                log.debug('set stats to {0}'.format(bucketInfo.stats.ram))
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return bucketInfo

    def get_vbuckets(self, bucket='default'):
        return self.get_bucket(bucket).vbuckets

    def delete_bucket(self, bucket='default'):
        api = '{0}{1}{2}'.format(self.baseUrl, '/pools/default/buckets/', bucket)
        try:
            response, content = httplib2.Http().request(api, 'DELETE', headers=self._create_headers())
            if response['status'] == '200':
                return True
            else:
                log.error('delete_bucket error {0}'.format(content))
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return False

    # figure out the proxy port
    def create_bucket(self, bucket='',
                      ramQuotaMB=1,
                      authType='none',
                      saslPassword='',
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase'):
        api = '{0}{1}'.format(self.baseUrl, '/pools/default/buckets')
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

        try:
            log.info("{0} with param: {1}".format(api, params))
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            log.info(content)
            log.info(response)
            status = response['status']
            if status == '200' or status == '201' or status == '202':
                return True
            else:
                log.error('create_bucket error {0} {1}'.format(content, response))
                raise BucketCreationException(ip=self.ip, bucket_name=bucket)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    #return AutoFailoverSettings
    def get_autofailover_settings(self):
        api = self.baseUrl + 'settings/autoFailover'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            log.info("settings/autoFailover response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('settings/autoFailover error {0}'.format(content))
                return None
            elif response['status'] == '200':
                parsed = json.loads(content)
                settings = AutoFailoverSettings()
                settings.enabled = parsed["enabled"]
                settings.count = parsed["count"]
                settings.timeout = parsed["timeout"]
                return settings
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

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
        log.info('settings/autoFailover params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("settings/autoFailover response {0}".format(response, content))
            if response['status'] == '400':
                log.error('settings/autoFailover error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def reset_autofailover(self):
        api = self.baseUrl + 'settings/autoFailover/resetCount'

        try:
            response, content = httplib2.Http().request(api, 'POST', '', headers=self._create_headers())
            log.info("settings/autoFailover/resetCount response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('reset_autofailover error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def enable_autofailover_alerts(self, recipients, sender, email_username, email_password, email_host='localhost',
                                   email_port=25, email_encrypt='false',
                                   alerts='auto_failover_node,auto_failover_maximum_reached'):
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
        log.info('settings/alerts params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("settings/alerts response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('enable_autofailover_alerts error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def disable_autofailover_alerts(self):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.urlencode({'enabled': 'false'})
        log.info('settings/alerts params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("settings/alerts response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('enable_autofailover_alerts error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def stop_rebalance(self):
        api = self.baseUrl + '/controller/stopRebalance'
        try:
            response, content = httplib2.Http().request(api, method='POST', headers=self._create_headers())
            if response['status'] == '400':
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def set_data_path(self, data_path):
        api = self.baseUrl + '/nodes/self/controller/settings'
        params = urllib.urlencode({'path': data_path})
        log.info('/nodes/self/controller/settings params : {0}'.format(params))

        try:
            response, content = httplib2.Http().request(api, 'POST', params, headers=self._create_headers())
            log.info("/nodes/self/controller/settings response {0} ,content {1}".format(response, content))
            if response['status'] == '400':
                log.error('set_data_path error {0}'.format(content))
                return False
            elif response['status'] == '200':
                return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


class MembaseServerVersion:
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
        self.availableStorage = None # list
        self.memoryQuota = None


class NodeDataStorage(object):
    def __init__(self):
        self.type = '' #hdd or ssd
        self.path = ''
        self.quotaMb = ''
        self.state = '' #ok

    def __str__(self):
        return '{0}'.format({'type': self.type,
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
                dict = parsed['availableStorage']
                if 'path' in dict and 'sizeKBytes' in dict and 'usagePercent' in dict:
                    diskStorage = NodeDiskStorage()
                    diskStorage.path = dict['path']
                    diskStorage.sizeKBytes = dict['sizeKBytes']
                    diskStorage.type = key
                    diskStorage.usagePercent = dict['usagePercent']
                    node.availableStorage.append(diskStorage)
                    log.info(diskStorage)

        if 'storage' in parsed:
            storage = parsed['storage']
            for key in storage:
                disk_storage_list = storage[key]
                for dict in disk_storage_list:
                    if 'path' in dict and 'state' in dict and 'quotaMb' in dict:
                        dataStorage = NodeDataStorage()
                        dataStorage.path = dict['path']
                        dataStorage.quotaMb = dict['quotaMb']
                        dataStorage.state = dict['state']
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
                                vbucketInfo.replica.append(serverList[vbucket[i]])
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
        log.debug('read {0} vbuckets'.format(len(bucket.vbuckets)))
        stats = parsed['basicStats']
        #vBucketServerMap
        bucketStats = BucketStats()
        log.debug('stats:{0}'.format(stats))
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
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
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