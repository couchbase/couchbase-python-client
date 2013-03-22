cimport couchbase as lcb

cdef extern from "string.h":
    void *memset(void *b, int c, size_t len)

cdef class Couchbase:
    cdef lcb.lcb_t _instance
    cdef lcb.lcb_create_st _create_options
    def __cinit__(self):
        memset(&self._create_options, 0, sizeof(self._create_options))

    def __init__(self, options):
        host = options.encode('utf-8')
        self._create_options.v.v0.host = host
        print("options", options)
        #print("create_options", create_options.v.v0.host)

        err = lcb.lcb_create(&self._instance, &self._create_options)
        if err != lcb.LCB_SUCCESS:
            print("create error", err)
        lcb.lcb_behavior_set_syncmode(self._instance, lcb.LCB_SYNCHRONOUS)

    def connect(self):
        err = lcb.lcb_connect(self._instance)
        if err != lcb.LCB_SUCCESS:
            print("connect error", err)

    def set(self, key, value):
        key = key.encode('utf-8')
        cdef char *c_key = key

        if isinstance(value, unicode):
           value = value.encode('utf-8')
        cdef char *c_value = value

        cdef lcb.lcb_store_cmd_t cmd
        cdef lcb.lcb_store_cmd_t *commands[1]
        commands[0] = &cmd
        memset(&cmd, 0, sizeof(cmd))
        cmd.v.v0.operation = lcb.LCB_SET
        cmd.v.v0.key = c_key
        cmd.v.v0.nkey = len(key)
        cmd.v.v0.bytes = c_value
        cmd.v.v0.nbytes = len(value)
        err = lcb.lcb_store(self._instance, NULL, 1, <lcb.lcb_store_cmd_t **>commands)
        if err != lcb.LCB_SUCCESS:
            print("store error", err)
