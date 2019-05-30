
"""


Key/Value Service (Memcached)
Core exception: KeyValueException
Generic (Shared) Exceptions:
TemporaryFailureException: handles the follow Memcached server errors
Temporary Failure (0x0086) - A temporary error has occurred in the server.
Out of Memory (0x0082) -  The server is temporarily out of memory.
Server Busy (0x0085) - The server was too busy to complete the operation.
TimeoutException
AuthenticationException
AuthenticationError (0x0020) - connection to memcached could not be authenticated
Eaccess (0x24) - no access for a given user on a resource
ServiceNotAvailableException - service is down or unreachable.
ServiceNotConfigured - service has not been configured on a cluster.
Specific Exceptions:
Basic Memcached
KeyNotFoundException (0x0001) -  The key does not exist in the database
KeyExistsException (0x0002) - The key already exists in the database
LockedException (0x09) - the key is locked
ValueTooLargeException (0x0003) - The value of the object stored was too large.
InvalidArgumentsException (0x0004) -  The arguments of the operation were invalid.
Sub-Document
PathNotFoundException (0xc0) - supplied path is invalid within the JSON document
PathMismatchException (0xc1) - the path component does not match the type of the element requested. For example, using a path that is an array on a non-array element; arithmetic operation on path whose value is not a number.
PathInvalidException: (0xc2) - the path provided is invalid. For operations requiring an array index, this is returned if the last component of that path isn't an array. Similarly for operations requiring a dictionary, if the last component isn't a dictionary but eg. an array index
PathTooDeepException (0xc3) - Subdocument error indicating that the path is too large (ie. the string is too long) or too deep (more than 32 components).
DocumentTooDeepException (0xc4) - Subdocument error indicating that the target document's level of JSON nesting is too deep to be processed by the subdoc service.
ValueTooDeepException (0xca) - Subdocument error indicating that, in a multi-specification, an invalid combination of commands were specified,  including the case where too many paths were specified.
CannotInsertValueException (0xc5) - Subdocument error indicating that the target document is not flagged or recognized as JSON.
DocumentNotJsonException (0xc6) - Subdocument error indicating that the existing document is not valid JSON.
NumRangeException (NumberTooBigException) (0xc7) -  Subdocument error indicating that for arithmetic subdoc operations, the existing number is out of the valid range.
DeltaRangeException (BadDeltaException) (0xc8) - Subdocument error indicating that for arithmetic subdoc operations, the operation will make the value out of valid range.
PathExistsException (0xc9) - Subdocument error indicating that the last component of the path already exist despite the mutation operation expecting it not to exist (the mutation was expecting to create only the last part of the path and store the fragment there).
InvalidArgumentsException (0xcb) - Subdocument error indicating that, in a multi-specification, an invalid combination of commands were specified, including the case where too many paths were specified.
XattrUnknownMacroException (0xd0) - The server has no knowledge of the requested macro.
CasMismatchException (NA) - the CAS value has changed for the key.
    Durability:
DurabilityInvalidLevel (0xa0) - the requested durability level is invalid
DurabilityImpossible (0xa1) - requested durability level is impossible given the cluster topology due to insufficient replica servers.
SyncWriteInProgress (0xa2) - attempt to mutate a key which has a SyncWrite pending. Client should retry, possibly with backoff.
    SyncWriteAmbiguous (0xa3) - SyncWrite request has not completed and the result, whether it succeeded or failed, is ambiguous or not known.
NoReplicasFound (0x0300) - The client could not locate a replica within the cluster map or replica read. The Bucket may not be configured to have replicas, which should be checked to ensure replica reads.
DocumentMutationLost (0x0600) - document mutation was lost during a hard failover.
DocumentMutationDetected (0x0601) - document mutation was detected on the document being observed.
Internal Exceptions (Generally Specific):
InternalException:
SubDocMultiPathFailure (0xcc) - Subdocument error indicating that, in a multi-specification, one or more commands failed to execute on a document which exists (ie. the key was valid).
SubDocXattrInvalidFlagCombo (0xce) - Subdocument error indicating the flag combination for an XATTR operation was invalid.
SubDocXattrInvalidKeyCombo (0xcf) - Subdocument error indicating the key combination for an XATTR operation was invalid.
SubdocXattrUnknownVattr (0xd1) - The server has no knowledge of the requested virtual xattr.
SubdocXattrCantModifyVattr (0xd2) - Virtual xattrs can't be modified
SubdocMultiPathFailureDeleted (0xd3) - Specified key was found as a Deleted document, but one or more path operations failed. Examine the individual lookup_result (MULTI_LOOKUP) mutation_result (MULTI_MUTATION) structures for details.
    SubdocInvalidXattrOrder (0xd4) - According to the spec all xattr commands should come first, followed by the commands for the document body.
Rollback (?)
VBucketBelongsToAnotherServer (0x0007) -The VBucket the operation was attempted on, no longer belongs to the server.
AuthenticationContinue (0x0021) - During SASL authentication, another step (or more) must be made before authentication is complete.
AuthStale (0x1f) - The authentication context is stale. You should reauthenticate
InternalError (0x0084) - An internal error has occurred.
UnknownCommand (0x0081) - The server received an unknown command from a client.
BucketNotConnected (0x0008) - bucket not connected.
UnknownError (?)
NotSupported (0x0083) - The operation is not supported.
NotInitialized (0x25) - The Couchbase cluster is currently initializing this node, and the Cluster manager has not yet granted all users access to the cluster.

Query (N1QL) Service
The Query Service breaks errors down to service (1xxx), parse (3xxx), plan (4xxx), general (5000/9999), execution (5xxx), authentication (10xxx), Data store system (11xxx), Couchbase datastore (12xxx), Datastore View (13xxx), Datastore GSI (14xxx), Datastore files (15xxx), and Datastore aspects (16xxx).
Core exception: QueryException
Generic (Shared) Exceptions:
TemporaryFailureException
TimeoutException
AuthenticationException
ServiceNotAvailableException - service is down or unreachable.
ServiceNotConfigured - service has not been configured on a cluster.
Specific Exceptions


Internal Exceptions
Analytics Service
Broken down into authorization (20xxx), API errors (21xxx), connection (22xxx), runtime (23xxx), compilation (24xxx), and internal errors (25xxx).
Core exception: AnalyticsException
Generic (Shared) Exceptions:
TemporaryFailureException
TimeoutException
AuthenticationException
Unauthorized (20000) - Unauthorized user.
Permission (20001) - user must have permission to perform an operation.
ServiceNotAvailableException - service is down or unreachable.
ServiceNotConfigured - service has not been configured on a cluster.
Specific Exceptions
Internal Exceptions
InternalException (25000) - Internal Error
Full Text Search (FTS) Service
Core exception: FtsException
Generic (Shared) Exceptions:
TemporaryFailureException
TimeoutException
AuthenticationException
ServiceNotAvailableException - service is down or unreachable.
ServiceNotConfigured - service has not been configured on a cluster.
Specific Exceptions


Internal Exceptions
View Service
Core exception: ViewException
Generic (Shared) Exceptions:
TemporaryFailureException
TimeoutException
AuthenticationException
ServiceNotAvailableException - service is down or unreachable.
ServiceNotConfigured - service has not been configured on a cluster.
Specific Exceptions


Internal Exceptions

"""


