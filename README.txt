example :

# TODO: the constructor should take "http://ip:8091/pools/","bucket"
# currently the constuctor takes a python dictionary
# server = {"ip":"localhost","port":8091,"username":"Administrator","password":"password"}
# bucket = "default"
# v = VBucketAwareMembaseClient(server,bucket)
# v.set("key1",0,0,"value1")
# v.get("key1")