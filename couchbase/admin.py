from couchbase.options import OptionBlock
from couchbase_core.admin import Admin


class InsertCollectionOptions(OptionBlock):
    pass


class InsertScopeOptions(OptionBlock):
    pass


class CollectionManager(object):
    def __init__(self,
                 admin_bucket,  # type: Admin
                 bucket_name  # type: str
                 ):
        self.admin_bucket = admin_bucket
        self.bucket_name = bucket_name

    def insert_collection(self,
                          collection_name,  # type: str
                          scope_name,  # type: str
                          *options  # type: InsertCollectionOptions
                          ):
        """
        Upsert a collection into the parent bucket

        :param collection_name: Collection name
        :param scope_name: Scope name
        :param options:
        :return:
        """

        path = "pools/default/buckets/default/collections/{}".format(scope_name)

        params = {
            'name': collection_name
        }

        form = self.admin_bucket._mk_formstr(params)
        return self.admin_bucket.http_request(path=path,
                                 method='POST',
                                 content_type='application/x-www-form-urlencoded',
                                 content=form)

    def insert_scope(self,
                     scope_name,  # type: str
                     *options  # type: InsertScopeOptions
                     ):
        """
        Upsert a collection into the parent bucket

        :param scope_name: Scope name
        :param options:
        :return:
        """

        path = "pools/default/buckets/default/collections"

        params = {
            'name': scope_name
        }

        form = self.admin_bucket._mk_formstr(params)
        return self.admin_bucket.http_request(path=path,
                                 method='POST',
                                 content_type='application/x-www-form-urlencoded',
                                 content=form)
