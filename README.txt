example :

smartclient = VBucketAwareCouchbaseClient
# the constructor should take "http://10.20.30.40:8091/pools/","bucket"
# currently the constuctor takes a python dictionary
# v = VBucketAwareCouchbaseClient("http://10.20.30.40:8091/pools/default","default")
# v.set("key1",0,0,"value1")
# v.get("key1")

# server = {"ip":"localhost","port":8091,"username":"Administrator","password":"password"}
rest management api : RestConnection(server)
