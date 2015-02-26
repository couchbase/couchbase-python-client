#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import time

import couchbase._libcouchbase as _LCB
import couchbase.exceptions as exceptions
from couchbase.exceptions import CouchbaseError, ArgumentError
from couchbase.views.params import Query, SpatialQuery, STALE_OK
from couchbase._pyport import single_dict_key

class BucketManager(object):
    """
    The `BucketManager` class allows access to common maintenance APIs related
    to a :class:`~couchbase.bucket.Bucket` object. It is normally returned via
    the :meth:`~couchbase.bucket.Bucket.bucket_manager` method
    """
    def __init__(self, cb):
        self._cb = cb

    def _http_request(self, **kwargs):
        return self._cb._http_request(**kwargs)

    def _mk_devmode(self, *args):
        return self._cb._mk_devmode(*args)

    def _view(self, *args, **kwargs):
        return self._cb._view(*args, **kwargs)

    def _doc_rev(self, res):
        """
        Returns the rev id from the header
        """
        jstr = res.headers['X-Couchbase-Meta']
        jobj = json.loads(jstr)
        return jobj['rev']

    def _poll_vq_single(self, dname, use_devmode, ddresp):
        """
        Initiate a view query for a view located in a design document
        :param ddresp: The design document to poll (as JSON)
        :return: True if successful, False if no views.
        """
        vname = None
        query = None
        v_mr = ddresp.get('views', {})
        v_spatial = ddresp.get('spatial', {})
        if v_mr:
            vname = single_dict_key(v_mr)
            query = Query()
        elif v_spatial:
            vname = single_dict_key(v_spatial)
            query = SpatialQuery()

        if not vname:
            return False

        query.stale = STALE_OK
        query.limit = 1

        for r in self._cb.query(dname, vname, use_devmode=use_devmode,
                                query=query):
            pass
        return True

    def _design_poll(self, name, mode, oldres, timeout=5, use_devmode=False):
        """
        Poll for an 'async' action to be complete.
        :param string name: The name of the design document
        :param string mode: One of ``add`` or ``del`` to indicate whether
            we should check for addition or deletion of the document
        :param oldres: The old result from the document's previous state, if
            any
        :param float timeout: How long to poll for. If this is 0 then this
            function returns immediately
        :type oldres: :class:`~couchbase.result.HttpResult`
        """
        if not timeout:
            return True

        if timeout < 0:
            raise ArgumentError.pyexc("Interval must not be negative")

        t_end = time.time() + timeout
        old_rev = None

        if oldres:
            old_rev = self._doc_rev(oldres)

        while time.time() < t_end:
            try:
                cur_resp = self.design_get(name, use_devmode=use_devmode)
                if old_rev and self._doc_rev(cur_resp) == old_rev:
                    continue

                try:
                    if not self._poll_vq_single(
                            name, use_devmode, cur_resp.value):
                        continue
                    return True

                except CouchbaseError:
                    continue

            except CouchbaseError:
                if mode == 'del':
                    # Deleted, whopee!
                    return True

        raise exceptions.TimeoutError.pyexc(
            "Wait time for design action completion exceeded")

    def design_create(self, name, ddoc, use_devmode=True, syncwait=0):
        """
        Store a design document

        :param string name: The name of the design
        :param ddoc: The actual contents of the design document

        :type ddoc: string or dict
            If ``ddoc`` is a string, it is passed, as-is, to the server.
            Otherwise it is serialized as JSON, and its ``_id`` field is set to
            ``_design/{name}``.

        :param bool use_devmode:
            Whether a *development* mode view should be used. Development-mode
            views are less resource demanding with the caveat that by default
            they only operate on a subset of the data. Normally a view will
            initially be created in 'development mode', and then published
            using :meth:`design_publish`

        :param float syncwait:
            How long to poll for the action to complete. Server side design
            operations are scheduled and thus this function may return before
            the operation is actually completed. Specifying the timeout here
            ensures the client polls during this interval to ensure the
            operation has completed.

        :raise: :exc:`couchbase.exceptions.TimeoutError` if ``syncwait`` was
            specified and the operation could not be verified within the
            interval specified.

        :return: An :class:`~couchbase.result.HttpResult` object.

        .. seealso:: :meth:`design_get`, :meth:`design_delete`,
            :meth:`design_publish`

        """
        name = self._cb._mk_devmode(name, use_devmode)

        fqname = "_design/{0}".format(name)
        if not isinstance(ddoc, dict):
            ddoc = json.loads(ddoc)

        ddoc = ddoc.copy()
        ddoc['_id'] = fqname
        ddoc = json.dumps(ddoc)

        existing = None
        if syncwait:
            try:
                existing = self.design_get(name, use_devmode=False)
            except CouchbaseError:
                pass

        ret = self._cb._http_request(
            type=_LCB.LCB_HTTP_TYPE_VIEW, path=fqname,
            method=_LCB.LCB_HTTP_METHOD_PUT, post_data=ddoc,
            content_type="application/json")

        self._design_poll(name, 'add', existing, syncwait,
                          use_devmode=use_devmode)
        return ret

    def design_get(self, name, use_devmode=True):
        """
        Retrieve a design document

        :param string name: The name of the design document
        :param bool use_devmode: Whether this design document is still in
            "development" mode

        :return: A :class:`~couchbase.result.HttpResult` containing
            a dict representing the format of the design document

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist.

        .. seealso:: :meth:`design_create`

        """
        name = self._mk_devmode(name, use_devmode)

        existing = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                      path="_design/" + name,
                                      method=_LCB.LCB_HTTP_METHOD_GET,
                                      content_type="application/json")
        return existing

    def design_publish(self, name, syncwait=0):
        """
        Convert a development mode view into a production mode views.
        Production mode views, as opposed to development views, operate on the
        entire cluster data (rather than a restricted subset thereof).

        :param string name: The name of the view to convert.

        Once the view has been converted, ensure that all functions (such as
        :meth:`design_get`) have the ``use_devmode`` parameter disabled,
        otherwise an error will be raised when those functions are used.

        Note that the ``use_devmode`` option is missing. This is intentional
        as the design document must currently be a development view.

        :return: An :class:`~couchbase.result.HttpResult` object.

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist

        .. seealso:: :meth:`design_create`, :meth:`design_delete`,
            :meth:`design_get`
        """
        existing = self.design_get(name, use_devmode=True)
        rv = self.design_create(name, existing.value, use_devmode=False,
                           syncwait=syncwait)
        self.design_delete(name, use_devmode=True,
                           syncwait=syncwait)
        self._design_poll(name, 'add', None,
                          timeout=syncwait, use_devmode=False)
        return rv

    def design_delete(self, name, use_devmode=True, syncwait=0):
        """
        Delete a design document

        :param string name: The name of the design document to delete
        :param bool use_devmode: Whether the design to delete is a development
            mode design doc.

        :param float syncwait: Timeout for operation verification. See
            :meth:`design_create` for more information on this parameter.

        :return: An :class:`HttpResult` object.

        :raise: :exc:`couchbase.exceptions.HTTPError` if the design does not
            exist
        :raise: :exc:`couchbase.exceptions.TimeoutError` if ``syncwait`` was
            specified and the operation could not be verified within the
            specified interval.

        .. seealso:: :meth:`design_create`, :meth:`design_get`

        """
        name = self._mk_devmode(name, use_devmode)
        existing = None
        if syncwait:
            try:
                existing = self.design_get(name, use_devmode=False)
            except CouchbaseError:
                pass

        ret = self._http_request(type=_LCB.LCB_HTTP_TYPE_VIEW,
                                 path="_design/" + name,
                                 method=_LCB.LCB_HTTP_METHOD_DELETE)

        self._design_poll(name, 'del', existing, syncwait)
        return ret
