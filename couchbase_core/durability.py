from couchbase_core import CompatibilityEnum


class Durability(CompatibilityEnum):
    @classmethod
    def prefix(cls):
        return "LCB_DURABILITYLEVEL_"
    MAJORITY_AND_PERSIST_TO_ACTIVE = ()
    NONE = ()
    MAJORITY = ()
    PERSIST_TO_MAJORITY = ()

    #  PYCBC-956
    #  Give BucketManager paths for enum->str and str->enum

    def to_server_str(self):
        if self.name == 'MAJORITY_AND_PERSIST_TO_ACTIVE':
            return 'majorityAndPersistActive'
        elif  self.name == 'NONE':
            return 'none'
        elif  self.name == 'MAJORITY':
            return 'majority'
        elif self.name == 'PERSIST_TO_MAJORITY':
            return 'persistToMajority'
        else:
            return 'none'

    @classmethod
    def from_server_str(cls, value):
        if value == 'majorityAndPersistActive':
            return cls.MAJORITY_AND_PERSIST_TO_ACTIVE
        elif value == 'none':
            return cls.NONE
        elif value == 'majority':
            return cls.MAJORITY
        elif value == 'persistToMajority':
            return cls.PERSIST_TO_MAJORITY
        else:
            return cls.NONE