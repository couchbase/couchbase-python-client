from couchbase_core import CompatibilityEnum


class Durability(CompatibilityEnum):
    @classmethod
    def prefix(cls):
        return "LCB_DURABILITYLEVEL_"
    MAJORITY_AND_PERSIST_TO_ACTIVE = ()
    NONE = ()
    MAJORITY = ()
    PERSIST_TO_MAJORITY = ()