#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)

# These helpers are the single point of error-handling / teardown for the streaming row
# iterators (query, analytics, search, views) across all three API surfaces (acouchbase,
# couchbase, txcouchbase).  Each request class delegates its ``__anext__`` / ``__next__`` here.
#
# They operate on the request (``req``) by convention -- ``req`` must provide:
#   * ``_get_next_row()``                  -- fetch/deserialize the next row (blocking)
#   * ``_process_core_span(exc_val=None)`` -- end the observability span/meter (idempotent)
#   * ``_get_metadata()``                  -- read trailing metadata once streaming completes
#   * ``_done_streaming``                  -- terminal-state flag
# and, for the async helper additionally:
#   * ``_loop`` / ``_tp_executor``         -- event loop + executor used to offload the blocking fetch
#   * ``_finalize(exc_val=None)``          -- end observability *and* release the executor
#
# The crucial detail: ``asyncio.CancelledError`` (and ``KeyboardInterrupt`` / ``SystemExit``)
# derive from ``BaseException``, not ``Exception``.  The explicit ``except BaseException`` branch
# ensures cancellation never bypasses teardown -- the span/meter are always ended and the executor
# is always released -- while the exception is re-raised unchanged to preserve cancellation
# semantics.


def _internal_exception(message):
    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
    return exc_cls(message)


async def stream_anext(req, op_name):
    """
    **INTERNAL**

    Drive a single async streaming iteration for ``req`` and apply uniform teardown/error handling.
    ``op_name`` is the human-readable operation name used in diagnostic messages (e.g. ``'N1QL'``).
    """
    try:
        # this is a blocking operation, so it is offloaded to the request's executor
        row = await req._loop.run_in_executor(req._tp_executor, req._get_next_row)
        # We want to end the streaming op span once we have a response from the C++ core.
        # Unfortunately right now, that means we need to wait until we have the first row (or we
        # have an error).  As this is idempotent, it is safe to call for each row (it will only do
        # work for the first call).
        req._process_core_span()
        return row
    except asyncio.QueueEmpty:
        excptn = _internal_exception(f'Unexpected QueueEmpty exception caught when doing {op_name} query.')
        req._finalize(exc_val=excptn)
        raise excptn
    except StopAsyncIteration:
        req._done_streaming = True
        req._finalize()
        req._get_metadata()
        raise
    except CouchbaseException as ex:
        req._finalize(exc_val=ex)
        raise
    except Exception as ex:
        excptn = _internal_exception(str(ex))
        req._finalize(exc_val=excptn)
        raise excptn
    except BaseException as ex:
        # asyncio.CancelledError / KeyboardInterrupt / SystemExit -- see module note above.
        req._finalize(exc_val=ex)
        raise


def stream_next(req):
    """
    **INTERNAL**

    Drive a single synchronous streaming iteration for ``req`` and apply uniform teardown/error
    handling.  Shared by the blocking (``couchbase``) and Twisted (``txcouchbase``) row iterators.
    """
    try:
        row = req._get_next_row()
        # See stream_anext for why the span is ended here (idempotent, ends on the first row).
        req._process_core_span()
        return row
    except StopIteration:
        req._done_streaming = True
        req._process_core_span()
        req._get_metadata()
        raise
    except CouchbaseException as ex:
        req._process_core_span(exc_val=ex)
        raise
    except Exception as ex:
        excptn = _internal_exception(str(ex))
        req._process_core_span(exc_val=excptn)
        raise excptn
    except BaseException as ex:
        # KeyboardInterrupt / SystemExit -- see module note above.
        req._process_core_span(exc_val=ex)
        raise
