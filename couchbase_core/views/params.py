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

# This module is largely used by other modules, though it just contains
# simple string utilities :)

import json
from copy import deepcopy

from couchbase_core._pyport import long, ulp, basestring
from couchbase.exceptions import InvalidArgumentException

# Some constants
STALE_UPDATE_BEFORE = "false"
STALE_UPDATE_AFTER = "update_after"
STALE_OK = "ok"
ONERROR_CONTINUE = "continue"
ONERROR_STOP = "stop"


class _Unspec(object):
    def __nonzero__(self):
        return False

    # Py3
    __bool__ = __nonzero__

    def __str__(self):
        return ""

    def __repr__(self):
        return "<Placeholder>"

UNSPEC = _Unspec()

def _bool_param_handler(input):
    if isinstance(input, bool):
        if input:
            return "true"
        else:
            return "false"

    if isinstance(input, basestring):
        if input not in ("true", "false"):
            raise InvalidArgumentException.pyexc("String for boolean must be "
                                      "'true' or 'false'", input)
        return input

    try:
        input + 0
        if input:
            return "true"
        else:
            return "false"

    except TypeError:
        raise InvalidArgumentException.pyexc("Boolean value must be boolean, "
                                  "numeric, or a string of 'true' "
                                  "or 'false'", input)


def _num_param_handler(input):
    # Don't allow booleans:
    if isinstance(input, bool):
        raise InvalidArgumentException.pyexc("Cannot use booleans as numeric values",
                                      input)
    try:
        return str(int(input))
    except Exception as e:
        raise InvalidArgumentException.pyexc("Expected a numeric argument", input, e)


def _string_param_common(input, do_quote=False):
    # TODO, if we pass this to urlencode, do we ever need to quote?
    # For the moment, i'm always forcing non-quote behavior
    do_quote = False

    s = None
    if isinstance(input, basestring):
        s = input

    elif isinstance(input, bool):
        raise InvalidArgumentException.pyexc("Can't use boolean as string", input)

    elif isinstance(input, (int, long, float)):
        # Basic numeric types:
        s = str(input)

    else:
        raise InvalidArgumentException.pyexc("Expected simple numeric type or string ",
                                      input)
    if do_quote:
        s = ulp.quote(s)

    return s


def _string_param_handler(input):
    return _string_param_common(input, do_quote=True)


def _generic_param_handler(input):
    return _string_param_handler(input, do_quote=False)


def _stale_param_handler(input):
    if input in (STALE_UPDATE_AFTER, STALE_OK, STALE_UPDATE_BEFORE):
        return input

    ret = _bool_param_handler(input)
    if ret == "true":
        ret = STALE_OK
    return ret


def _onerror_param_handler(input):
    if input not in (ONERROR_CONTINUE, ONERROR_STOP):
        raise InvalidArgumentException.pyexc(
            "on_error must be 'continue' or 'stop'", input)

    return input


def _jval_param_handler(input):
    try:
        ret = json.dumps(input)
        return _string_param_handler(ret)
    except Exception as e:
        raise InvalidArgumentException.pyexc("Couldn't convert value to JSON", input, e)


def _jarry_param_handler(input):
    ret = _jval_param_handler(input)
    if not ret.startswith('['):
        raise InvalidArgumentException.pyexc(
            "Value must be converted to JSON array", input)

    return ret


# Some more constants. Yippie!
class Params(object):
    # Random, unspecified value.

    DESCENDING              = "descending"
    STARTKEY                = "startkey"
    STARTKEY_DOCID          = "startkey_docid"
    ENDKEY                  = "endkey"
    ENDKEY_DOCID            = "endkey_docid"
    KEY                     = "key"
    KEYS                    = "keys"
    INCLUSIVE_END           = "inclusive_end"

    GROUP                   = "group"
    GROUP_LEVEL             = "group_level"
    REDUCE                  = "reduce"

    SKIP                    = "skip"
    LIMIT                   = "limit"

    ON_ERROR                = "on_error"
    STALE                   = "stale"
    DEBUG                   = "debug"
    CONNECTION_TIMEOUT      = "connection_timeout"
    FULL_SET                = "full_set"

    MAPKEY_SINGLE           = "mapkey_single"
    MAPKEY_MULTI            = "mapkey_multi"
    MAPKEY_RANGE            = "mapkey_range"
    DOCKEY_RANGE            = "dockey_range"
    START_RANGE             = "start_range"
    END_RANGE               = "end_range"

_HANDLER_MAP = {
    Params.DESCENDING        : _bool_param_handler,

    Params.STARTKEY          : _jval_param_handler,
    Params.STARTKEY_DOCID    : _string_param_handler,
    Params.ENDKEY            : _jval_param_handler,
    Params.ENDKEY_DOCID      : _string_param_handler,

    Params.FULL_SET          : _bool_param_handler,

    Params.GROUP             : _bool_param_handler,
    Params.GROUP_LEVEL       : _num_param_handler,
    Params.INCLUSIVE_END     : _bool_param_handler,
    Params.KEY               : _jval_param_handler,
    Params.KEYS              : _jarry_param_handler,
    Params.ON_ERROR          : _onerror_param_handler,
    Params.REDUCE            : _bool_param_handler,
    Params.STALE             : _stale_param_handler,
    Params.SKIP              : _num_param_handler,
    Params.LIMIT             : _num_param_handler,
    Params.DEBUG             : _bool_param_handler,
    Params.CONNECTION_TIMEOUT: _num_param_handler,
    Params.START_RANGE       : _jarry_param_handler,
    Params.END_RANGE         : _jarry_param_handler,
}


def _gendoc(param):
    for k, v in Params.__dict__.items():
        if param == v:
            return "\n:data:`Params.{0}`".format(k)


def _rangeprop(k_sugar, k_start, k_end):
    def getter(self):
        return self._user_options.get(k_sugar, UNSPEC)

    def setter(self, value):
        self._set_range_common(k_sugar, k_start, k_end, value)

    return property(getter, setter, fdel=None, doc=_gendoc(k_sugar))


def _genprop(p):
    def getter(self):
        return self._get_common(p)

    def setter(self, value):
        self._set_common(p, value)

    return property(getter, setter, fdel=None, doc=_gendoc(p))


class QueryBase(object):
    def _set_common(self, param, value, set_user=True):
        # Invalidate encoded string
        self._encoded = None

        if value is UNSPEC:
            self._real_options.pop(param, None)
            if set_user:
                self._user_options.pop(param, None)
            return

        handler = _HANDLER_MAP.get(param)
        if not handler:
            if not self.unrecognized_ok:
                raise InvalidArgumentException.pyexc(
                    "Unrecognized parameter. To use unrecognized parameters, "
                    "set 'unrecognized_ok' to True")

        if not handler:
            self._extra_options[param] = _string_param_handler(value)
            return

        if self.passthrough:
            handler = _string_param_handler

        self._real_options[param] = handler(value)
        if set_user:
            self._user_options[param] = value

    def _get_common(self, param):
        if param in self._user_options:
            return self._user_options[param]
        return self._real_options.get(param, UNSPEC)

    def _set_range_common(self, k_sugar, k_start, k_end, value):
        """
        Checks to see if the client-side convenience key is present, and if so
        converts the sugar convenience key into its real server-side
        equivalents.

        :param string k_sugar: The client-side convenience key
        :param string k_start: The server-side key specifying the beginning of
            the range
        :param string k_end: The server-side key specifying the end of the
            range
        """

        if not isinstance(value, (list, tuple, _Unspec)):
            raise InvalidArgumentException.pyexc(
                "Range specification for {0} must be a list, tuple or UNSPEC"
                .format(k_sugar))

        if self._user_options.get(k_start, UNSPEC) is not UNSPEC or (
                self._user_options.get(k_end, UNSPEC) is not UNSPEC):

            raise InvalidArgumentException.pyexc(
                "Cannot specify {0} with either {1} or {2}"
                .format(k_sugar, k_start, k_end))

        if not value:
            self._set_common(k_start, UNSPEC, set_user=False)
            self._set_common(k_end, UNSPEC, set_user=False)
            self._user_options[k_sugar] = UNSPEC
            return

        if len(value) not in (1, 2):
            raise InvalidArgumentException.pyexc("Range specification "
                                      "must have one or two elements",
                                          value)

        value = value[::]
        if len(value) == 1:
            value.append(UNSPEC)

        for p, ix in ((k_start, 0), (k_end, 1)):
            self._set_common(p, value[ix], set_user=False)

        self._user_options[k_sugar] = value

    STRING_RANGE_END = json.loads('"\u0FFF"')
    """
    Highest acceptable unicode value
    """

    def __init__(self, passthrough=False, unrecognized_ok=False, **params):
        """
        Create a new Query object.

        A Query object is used as a container for the various view options.
        It can be used as a standalone object to encode queries but is typically
        passed as the ``query`` value to :class:`~couchbase_core.views.iterator.View`.

        :param boolean passthrough:
            Whether *passthrough* mode is enabled

        :param boolean unrecognized_ok:
            Whether unrecognized options are acceptable. See
            :ref:`passthrough_values`.

        :param params:
            Key-value pairs for view options. See :ref:`view_options` for
            a list of acceptable options and their values.


        :raise: :exc:`couchbase.exceptions.InvalidArgumentException` if a view option
            or a combination of view options were deemed invalid.

        """
        self.passthrough = passthrough
        self.unrecognized_ok = unrecognized_ok
        self._real_options = {}
        self._user_options = {}
        self._extra_options = {}

        self._encoded = None

        # String literal to pass along with the query
        self._base_str = ""
        self.update(**params)

    def update(self, copy=False, **params):
        """
        Chained assignment operator.

        This may be used to quickly assign extra parameters to the
        :class:`Query` object.

        Example::

            q = Query(reduce=True, full_sec=True)

            # Someplace later

            v = View(design, view, query=q.update(mapkey_range=["foo"]))

        Its primary use is to easily modify the query object (in-place).

        :param boolean copy:
            If set to true, the original object is copied before new attributes
            are added to it
        :param params: Extra arguments. These must be valid query options.

        :return: A :class:`Query` object. If ``copy`` was set to true, this
            will be a new instance, otherwise it is the same instance on which
            this method was called

        """
        if copy:
            self = deepcopy(self)

        for k, v in params.items():
            if not hasattr(self, k):
                if not self.unrecognized_ok:
                    raise InvalidArgumentException.pyexc("Unknown option", k)
                self._set_common(k, v)

            else:
                setattr(self, k, v)

        return self

    @classmethod
    def from_any(cls, params, **ctor_opts):
        """
        Creates a new Query object from input.

        :param params: Parameter to convert to query
        :type params: dict, string, or :class:`Query`

        If ``params`` is a :class:`Query` object already, a deep copy is made
        and a new :class:`Query` object is returned.

        If ``params`` is a string, then a :class:`Query` object is contructed
        from it. The string itself is not parsed, but rather prepended to
        any additional parameters (defined via the object's methods)
        with an additional ``&`` characted.

        If ``params`` is a dictionary, it is passed to the :class:`Query`
        constructor.

        :return: a new :class:`Query` object
        :raise: :exc:`InvalidArgumentException` if the input is none of the acceptable
            types mentioned above. Also raises any exceptions possibly thrown
            by the constructor.

        """
        if isinstance(params, cls):
            return deepcopy(params)

        elif isinstance(params, dict):
            ctor_opts.update(**params)
            if cls is QueryBase:
                if ('bbox' in params or 'start_range' in params or
                            'end_range' in params):
                    return SpatialQuery(**ctor_opts)
                else:
                    return ViewQuery(**ctor_opts)

        elif isinstance(params, basestring):
            ret = cls()
            ret._base_str = params
            return ret

        else:
            raise InvalidArgumentException.pyexc("Params must be Query, dict, or string")

    @classmethod
    def from_string(cls, qstr):
        """Wrapper for :meth:`from_any`"""
        return cls.from_any(qstr)

    def _encode(self, omit_keys=False):
        res_d = []

        for k, v in self._real_options.items():
            if v is UNSPEC:
                continue

            if omit_keys and k == "keys":
                continue

            if not self.passthrough:
                k = ulp.quote(k)
                v = ulp.quote(v)

            res_d.append("{0}={1}".format(k, v))

        for k, v in self._extra_options.items():
            res_d.append("{0}={1}".format(k, v))

        return '&'.join(res_d)

    @property
    def encoded(self):
        """
        Returns an encoded form of the query
        """
        if not self._encoded:
            self._encoded = self._encode()

        if self._base_str:
            return '&'.join((self._base_str, self._encoded))

        else:
            return self._encoded

    @property
    def _long_query_encoded(self):
        """
        Returns the (uri_part, post_data_part) for a long query.
        """
        uristr = self._encode(omit_keys=True)
        kstr = "{}"

        klist = self._real_options.get('keys', UNSPEC)
        if klist != UNSPEC:
            kstr = '{{"keys":{0}}}'.format(klist)

        return (uristr, kstr)

    @property
    def has_blob(self):
        """
        Whether this query object is 'dirty'.

        A 'dirty' object is one which
        contains parameters unrecognized by the internal handling methods.
        A dirty query may be constructed by using the ``passthrough``
        or ``unrecognized_ok`` options, or by passing a string to
        :meth:`from_any`
        """
        return self._base_str or self.unrecognized_ok or self.passthrough

    def __repr__(self):
        return "Query:'{0}'".format(self.encoded)


    # Common parameters:
    stale               = _genprop(Params.STALE)
    skip                = _genprop(Params.SKIP)
    limit               = _genprop(Params.LIMIT)
    full_set            = _genprop(Params.FULL_SET)
    connection_timeout  = _genprop(Params.CONNECTION_TIMEOUT)
    debug               = _genprop(Params.DEBUG)
    on_error            = _genprop(Params.ON_ERROR)


class ViewQuery(QueryBase):
    descending          = _genprop(Params.DESCENDING)

    # Use the range parameters. They're easier
    startkey            = _genprop(Params.STARTKEY)
    endkey              = _genprop(Params.ENDKEY)
    startkey_docid      = _genprop(Params.STARTKEY_DOCID)
    endkey_docid        = _genprop(Params.ENDKEY_DOCID)

    keys                = _genprop(Params.KEYS)
    key                 = _genprop(Params.KEY)
    inclusive_end       = _genprop(Params.INCLUSIVE_END)

    reduce              = _genprop(Params.REDUCE)
    group               = _genprop(Params.GROUP)
    group_level         = _genprop(Params.GROUP_LEVEL)

    # Aliases:
    mapkey_single       = _genprop(Params.KEY)
    mapkey_multi        = _genprop(Params.KEYS)

    mapkey_range        = _rangeprop(Params.MAPKEY_RANGE,
                                     Params.STARTKEY, Params.ENDKEY)

    dockey_range        = _rangeprop(Params.DOCKEY_RANGE,
                                     Params.STARTKEY_DOCID,
                                     Params.ENDKEY_DOCID)


class SpatialQuery(QueryBase):
    start_range         = _genprop(Params.START_RANGE)
    end_range           = _genprop(Params.END_RANGE)


class Query(ViewQuery):
    pass


def make_options_string(input, unrecognized_ok=False, passthrough=False):
    if not isinstance(input, QueryBase):
        input = QueryBase.from_any(input, unrecognized_ok=unrecognized_ok,
                                   passthrough=passthrough)
    return input.encoded


def make_dvpath(doc, view):
    return "_design/{0}/_view/{1}?".format(doc, view)
