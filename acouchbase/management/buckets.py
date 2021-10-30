import asyncio
from typing import Any, Iterable

from couchbase.management.admin import Admin
from couchbase.options import forward_args

from couchbase_core import mk_formstr

from couchbase.management.buckets import (BucketManagerErrorHandler, CreateBucketSettings,
                                          CreateBucketOptions, BucketSettings, UpdateBucketOptions,
                                          DropBucketOptions, GetBucketOptions, GetAllBucketOptions, FlushBucketOptions)


@BucketManagerErrorHandler.wrap
class ABucketManager(object):
    _HANDLE_ERRORS_ASYNC = True

    def __init__(self,         # type: "ABucketManager"
                 admin_bucket  # type: Admin
                 ):
        """Bucket Manager

        :param admin_bucket: Admin bucket
        """
        self._admin_bucket = admin_bucket
        self._base_path = "/pools/default/buckets"

    def create_bucket(self,      # type: "ABucketManager"
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Any
                      ):
        """
        Creates a new bucket.

        :param: CreateBucketSettings settings: settings for the bucket.
        :param: CreateBucketOptions options: options for setting the bucket.
        :param: Any kwargs: override corresponding values in the options.

        :raises: BucketAlreadyExistsException
        :raises: InvalidArgumentsException
        """
        # prune the missing settings...
        params = settings.as_dict()

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', 0))

        # ensure replicaIndex is an int, if specified
        if 'replicaIndex' in params:
            params['replicaIndex'] = 1 if params['replicaIndex'] else 0

        # send it
        result = self._admin_bucket.http_request(
            path=self._base_path,
            method='POST',
            content=mk_formstr(params),
            content_type='application/x-www-form-urlencoded',
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def update_bucket(self,     # type: "ABucketManager"
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ):
        """
        Updates a bucket. Every setting must be set to what the user wants it to be after the update.
        Any settings that are not set to their desired values may be reverted to default values by the server.

        :param BucketSettings settings: settings for updating the bucket.
        :param UpdateBucketOptions options: options for updating the bucket.
        :param Any kwargs: override corresponding values in the options.

        :raises: InvalidArgumentsException
        :raises: BucketDoesNotExistException
        """

        # prune the missing settings...
        params = settings.as_dict()  # *options, **kwargs)

        # insure flushEnabled is an int
        params['flushEnabled'] = int(params.get('flushEnabled', 0))

        # send it
        result = self._admin_bucket.http_request(
            path="{}/{}".format(self._base_path, settings.name),
            method='POST',
            content_type='application/x-www-form-urlencoded',
            content=mk_formstr(params),
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def drop_bucket(self,         # type: "ABucketManager"
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ):
        # type: (...) -> None
        """
        Removes a bucket.

        :param str bucket_name: the name of the bucket.
        :param DropBucketOptions options: options for dropping the bucket.
        :param Any kwargs: override corresponding value in the options.

        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        result = self._admin_bucket.http_request(
            path="{}/{}".format(self._base_path, bucket_name),
            method='DELETE',
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def get_bucket(self,          # type: "ABucketManager"
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ):
        # type: (...) -> BucketSettings
        """
        Gets a bucket's settings.

        :param str bucket_name: the name of the bucket.
        :param GetBucketOptions options: options for getting the bucket.
        :param Any kwargs: override corresponding values in options.

        :returns: settings for the bucket. Note: the ram quota returned is in bytes
          not mb so requires x  / 1024 twice. Also Note: FlushEnabled is not a setting returned by the server, if flush is enabled then the doFlush endpoint will be listed and should be used to populate the field.

        :rtype: BucketSettings
        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        """
        result = self._admin_bucket.http_request(
            path="{}/{}".format(self._base_path, bucket_name),
            method='GET',
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(response):
            ft.set_result(BucketSettings.from_raw(response.value))
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def get_all_buckets(self,     # type: "ABucketManager"
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ):
        # type: (...) -> Iterable[BucketSettings]
        """
        Gets all bucket settings. Note,  # type: the ram quota returned is in bytes
        not mb so requires x  / 1024 twice.

        :param GetAllBucketOptions options: options for getting all buckets.
        :param Any kwargs: override corresponding value in options.

        :returns: An iterable of settings for each bucket.
        :rtype: Iterable[BucketSettings]
        """
        result = self._admin_bucket.http_request(
            path=self._base_path,
            method='GET',
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(response):
            ft.set_result(
                list(map(lambda b: BucketSettings(**b), response.value)))
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft

    def flush_bucket(self,          # type: "ABucketManager"
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ):
        # using the ns_server REST interface
        """
        Flushes a bucket (uses the ns_server REST interface).

        :param str bucket_name: the name of the bucket.
        :param FlushBucketOptions options: options for flushing the bucket.
        :param Any kwargs: override corresponding value in options.

        :raises: BucketNotFoundException
        :raises: InvalidArgumentsException
        :raises: FlushDisabledException
        """
        result = self._admin_bucket.http_request(
            path="/pools/default/buckets/{}/controller/doFlush".format(
                bucket_name),
            method='POST',
            **forward_args(kwargs, *options))

        ft = asyncio.Future()

        def on_ok(_):
            ft.set_result(True)
            result.clear_callbacks()

        def on_err(_, excls, excval, __):
            err = excls(excval)
            ft.set_exception(err)
            result.clear_callbacks()

        result.set_callbacks(on_ok, on_err)
        return ft
