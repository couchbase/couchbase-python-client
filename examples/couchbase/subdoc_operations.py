# @TODO: change back to subdocument to match 3.x???
import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster

#import couchbase.subdocument as SD
# **DEPRECATED**, use from couchbase.options import MutateInOptions
from couchbase.collection import MutateInOptions
from couchbase.durability import (ClientDurability,
                                  Durability,
                                  PersistTo,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import (CASMismatchException,
                                  CouchbaseException,
                                  DocumentExistsException,
                                  DurabilityImpossibleException,
                                  PathExistsException,
                                  PathNotFoundException,
                                  SubdocCantInsertValueException,
                                  SubdocPathMismatchException)

cluster = Cluster(
    "couchbase://localhost",
    authenticator=PasswordAuthenticator(
        "Administrator",
        "password"))
bucket = cluster.bucket("default")
collection = bucket.default_collection()

json_doc = {
    "name": "Douglas Reynholm",
    "email": "douglas@reynholmindustries.com",
    "addresses": {
        "billing": {
            "line1": "123 Any Street",
            "line2": "Anytown",
            "country": "United Kingdom"
        },
        "delivery": {
            "line1": "123 Any Street",
            "line2": "Anytown",
            "country": "United Kingdom"
        }
    },
    "purchases": {
        "complete": [
            339, 976, 442, 666
        ],
        "abandoned": [
            157, 42, 999
        ]
    }
}

try:
    collection.insert("customer123", json_doc)
except DocumentExistsException:
    collection.remove("customer123")
    collection.insert("customer123", json_doc)

result = collection.lookup_in("customer123",
                              [SD.get("addresses.delivery.country")])
country = result.content_as[str](0)  # "United Kingdom"
print(country)

result = collection.lookup_in(
    "customer123", [
        SD.exists("purchases.pending[-1]")])
print('Path exists: {}.'.format(result.exists(0)))
# Path exists:  False.

# NOTE:  result.content_as[bool](0) raises a PathNotFoundException
#        this is b/c when checking if a path exists
#        no content is returned

try:
    print("Path exists {}.".format(result.content_as[bool](0)))
except PathNotFoundException:
    print("Path does not exist")

result = collection.lookup_in(
    "customer123",
    [SD.get("addresses.delivery.country"),
     SD.exists("purchases.complete[-1]")])

print("{0}".format(result.content_as[str](0)))
print("Path exists: {}.".format(result.exists(1)))
# path exists: True.

collection.mutate_in("customer123", [SD.upsert("fax", "311-555-0151")])

collection.mutate_in(
    "customer123", [SD.insert("purchases.pending", [42, True, "None"])])

try:
    collection.mutate_in(
        "customer123", [
            SD.insert(
                "purchases.complete",
                [42, True, "None"])])
except PathExistsException:
    print("Path exists, cannot use insert.")

collection.mutate_in(
    "customer123",
    (SD.remove("addresses.billing"),
     SD.replace(
        "email",
        "dougr96@hotmail.com")))

# NOTE:  the mutate_in() operation expects a tuple or list
collection.mutate_in(
    "customer123", (SD.array_append(
                    "purchases.complete", 777),))
# purchases.complete is now [339, 976, 442, 666, 777]

collection.mutate_in(
    "customer123", [
        SD.array_prepend(
            "purchases.abandoned", 18)])
# purchases.abandoned is now [18, 157, 42, 999]

collection.upsert("my_array", [])
collection.mutate_in("my_array", [SD.array_append("", "some element")])
# the document my_array is now ["some element"]

collection.mutate_in(
    "my_array", [
        SD.array_append(
            "", "elem1", "elem2", "elem3")])

# the document my_array is now ["some_element", "elem1", "elem2", "elem3"]

collection.mutate_in(
    "my_array", [
        SD.array_append(
            "", ["elem4", "elem5", "elem6"])])

# the document my_array is now ["some_element", "elem1", "elem2", "elem3",
#                                   ["elem4", "elem5", "elem6"]]]

collection.mutate_in(
    "my_array", (SD.array_append("", "elem7"),
                 SD.array_append("", "elem8"),
                 SD.array_append("", "elem9")))


collection.upsert("some_doc", {})
collection.mutate_in(
    "some_doc", [
        SD.array_prepend(
            "some.array", "Hello", "World", create_parents=True)])
# the document some_doc is now {"some":{"array":["Hello", "World"]}}
try:
    collection.mutate_in(
        "customer123", [
            SD.array_addunique(
                "purchases.complete", 95)])
    print('Success!')
except PathExistsException:
    print('Path already exists.')

try:
    collection.mutate_in(
        "customer123", [
            SD.array_addunique(
                "purchases.complete", 95)])
    print('Success!')
except PathExistsException:
    print('Path already exists.')

# cannot add JSON obj w/ array_addunique
try:
    collection.mutate_in(
        "customer123", [
            SD.array_addunique(
                "purchases.complete", {"new": "object"})])
except SubdocCantInsertValueException as ex:
    print("Cannot add JSON value w/ array_addunique.", ex)


# cannot use array_addunique if array contains JSON obj
collection.mutate_in(
    "customer123", [
        SD.upsert(
            "purchases.cancelled", [{"Date": "Some date"}])])

try:
    collection.mutate_in(
        "customer123", [
            SD.array_addunique(
                "purchases.cancelled", 89)])
except SubdocPathMismatchException as ex:
    print("Cannot use array_addunique if array contains JSON objs.", ex)

collection.upsert("array", [])
collection.mutate_in("array", [SD.array_append("", "hello", "world")])
collection.mutate_in("array", [SD.array_insert("[1]", "cruel")])

# exception raised if attempt to insert in out of bounds position
try:
    collection.mutate_in("array", [SD.array_insert("[6]", "!")])
except PathNotFoundException:
    print("Cannot insert to out of bounds index.")

# # can insert into nested arrays as long as the path is appropriate
collection.mutate_in("array", [SD.array_append("", ["another", "array"])])
collection.mutate_in("array", [SD.array_insert("[3][2]", "!")])


result = collection.mutate_in("customer123", (SD.counter("logins", 1),))
num_logins = collection.get("customer123").content_as[dict]["logins"]
print('Number of logins: {}.'.format(num_logins))
# Number of logins: 1.

collection.upsert("player432", {"gold": 1000})

collection.mutate_in("player432", (SD.counter("gold", -150),))
result = collection.lookup_in("player432", (SD.get("gold"),))
print("{} has {} gold remaining.".format(
    "player432", result.content_as[int](0)))
# player432 has 850 gold remaining.

collection.mutate_in("customer123", [SD.upsert("level_0.level_1.foo.bar.phone",
                                               dict(
                                                   num="311-555-0101",
                                                   ext=16
                                               ), create_parents=True)])

collection.mutate_in(
    "customer123", [SD.array_append("purchases.complete", 998)])

collection.mutate_in(
    "customer123", [SD.array_append("purchases.complete", 999)])

try:
    collection.mutate_in(
        "customer123", [SD.array_append("purchases.complete", 999)],
        MutateInOptions(cas=1234))
except (DocumentExistsException, CASMismatchException) as ex:
    # we expect an exception here as the CAS value is chosen
    # for example purposes
    print(ex)

# @TODO: couchbase++ doesn't implement observe based durability
# try:
#     collection.mutate_in(
#         "key", [SD.insert("username", "dreynholm")],
#         MutateInOptions(durability=ClientDurability(
#                         ReplicateTo.ONE,
#                         PersistTo.ONE)))
# except CouchbaseException as ex:
#     print('Need to have more than 1 node for durability')
#     print(ex)

try:
    collection.mutate_in(
        "customer123", [SD.insert("username", "dreynholm")],
        MutateInOptions(durability=ServerDurability(
                        Durability.MAJORITY)))
except DurabilityImpossibleException as ex:
    print('Need to have more than 1 node for durability')
    print(ex)
