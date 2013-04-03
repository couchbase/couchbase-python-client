from libc.stdlib cimport malloc, free
from libc.string cimport memset

cimport couchbase as lcb


# Pure Python imports
import json
import pickle


cdef public enum _cb_formats:
    CB_FMT_JSON = 0x0
    CB_FMT_PICKLE = 0x1
    CB_FMT_PLAIN = 0x2
    CB_FMT_MASK = 0x3


cdef void cb_get_callback(lcb.lcb_t instance, const void *cookie,
                          lcb.lcb_error_t error,
                          const lcb.lcb_get_resp_t *resp):
    # A lot of error handling is missing here, but you get at least your
    # values back
    ctx = <object>cookie

    key = (<char *>resp.v.v0.key)[:resp.v.v0.nkey].decode('utf-8')
    flags = resp.v.v0.flags
    val = None

    if resp.v.v0.nbytes != 0:
        raw = (<char *>resp.v.v0.bytes)[:resp.v.v0.nbytes]
        format = flags & CB_FMT_MASK
        val = Utils.decode_value(raw, format)

    ctx['rv'].append((key, val))



class Utils:
    @staticmethod
    def context_dict():
        return {
            'rv': []
        }

    @staticmethod
    def encode_value(raw, format):
        if format == CB_FMT_JSON:
           return json.dumps(raw).encode('utf-8')
        elif format == CB_FMT_PICKLE:
            return pickle.dumps(raw)
        elif format == CB_FMT_PLAIN:
            return raw
        else:
            # Unknown formats are treated as plain
            return raw

    @staticmethod
    def decode_value(raw, format):
        if format == CB_FMT_JSON:
            return json.loads(raw.decode('utf-8'))
        elif format == CB_FMT_PICKLE:
            return pickle.loads(raw)
        elif format == CB_FMT_PLAIN:
            return raw
        else:
            # Unknown formats are treated as plain
            return raw


cdef class Couchbase:
    cdef lcb.lcb_t _instance
    cdef lcb.lcb_create_st _create_options
    cdef public lcb.lcb_uint32_t default_format
    def __cinit__(self):
        self.default_format = CB_FMT_JSON
        memset(&self._create_options, 0, sizeof(self._create_options))

    def __init__(self, options):
        host = options.encode('utf-8')
        self._create_options.v.v0.host = host
        print("options", options)

        err = lcb.lcb_create(&self._instance, &self._create_options)
        if err != lcb.LCB_SUCCESS:
            print("create error", err)
        lcb.lcb_behavior_set_syncmode(self._instance, lcb.LCB_SYNCHRONOUS)

    def connect(self):
        err = lcb.lcb_connect(self._instance)
        if err != lcb.LCB_SUCCESS:
            print("connect error", err)

    def set(self, key, value, format=None):
        key = key.encode('utf-8')
        cdef char *c_key = key

        if format is None:
            format = self.default_format

        value = Utils.encode_value(value, format)
        cdef char *c_value = value

        cdef int num_commands = 1
        cdef lcb.lcb_store_cmd_t cmd
        cdef const lcb.lcb_store_cmd_t **commands = \
            <const lcb.lcb_store_cmd_t **>malloc(
                num_commands * sizeof(lcb.lcb_store_cmd_t *))
        if not commands:
            raise MemoryError()

        try:
            commands[0] = &cmd
            memset(&cmd, 0, sizeof(cmd))
            cmd.v.v0.operation = lcb.LCB_SET
            cmd.v.v0.key = c_key
            cmd.v.v0.nkey = len(key)
            cmd.v.v0.bytes = c_value
            cmd.v.v0.nbytes = len(value)
            cmd.v.v0.flags = format
            err = lcb.lcb_store(self._instance, NULL, 1, commands)
            if err != lcb.LCB_SUCCESS:
                print("store error", err)
        finally:
            free(commands)

    def get(self, key):
        key = key.encode('utf-8')
        cdef char *c_key = key

        ctx = Utils.context_dict()
        <void>lcb.lcb_set_get_callback(self._instance, cb_get_callback);

        cdef int num_commands = 1
        cdef lcb.lcb_get_cmd_t cmd
        cdef const lcb.lcb_get_cmd_t **commands = \
            <const lcb.lcb_get_cmd_t **>malloc(
                num_commands * sizeof(lcb.lcb_get_cmd_t *))
        if not commands:
            raise MemoryError()

        try:
            commands[0] = &cmd
            memset(&cmd, 0, sizeof(cmd))
            cmd.v.v0.key = c_key
            cmd.v.v0.nkey = len(key)
            err = lcb.lcb_get(self._instance, <void *>ctx, 1, commands)
            if err != lcb.LCB_SUCCESS:
                print("get error", err)

            return ctx['rv']
        finally:
            free(commands)
