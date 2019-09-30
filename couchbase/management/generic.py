from typing import *

from couchbase_core.admin import Admin


class GenericManager(object):
    def __init__(self,
                 admin_bucket  # type: Admin
                 ):
        self._admin_bucket = admin_bucket