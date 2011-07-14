import uuid
from membaseclient import VBucketAwareMemcachedClient


server = {"ip":"10.1.5.38",
          "port":8091,
          "rest_username":"Administrator",
          "rest_password":"password",
          "username":"Administrator",
          "password":"password"}
v = VBucketAwareMemcachedClient(server,"default")
v.memcached("hi").set("hi", 0, 0, "hi")
for i in range(0, 1000000):
    key = str(uuid.uuid4())
    a, b, c = v.set(key, 0, 0, "hi")
    v.add(key,2,0,"hi")
#    a, b, c = v.get(key)
