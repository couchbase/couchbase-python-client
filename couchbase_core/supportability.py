import warnings

from functools import wraps

from couchbase_core import operation_mode


def deprecate_module_attribute(mod, deprecated=tuple()):
    return warn_on_attribute_access(mod, deprecated, "deprecated")


class Level(object):
    desc = None  # type: str

    msg_params = "msg_params"

    def __new__(cls, function, *args, **kwargs):
        """
        Mark a function as {}

        :param function: input function
        :return: marked function
        """.format(cls.__name__)

        message = cls.desc+"\n"
        msg_params = kwargs.get(cls.msg_params)
        if msg_params:
            message = message.format(**msg_params)

        func_name = getattr(function, '__qualname__', function.__name__)

        result = cls.get_final_fn(function, message, func_name)
        operation_mode.operate_on_doc(result,
                                      lambda x:
                                      (function.__doc__+"\n\n" if function.__doc__ else "") + \
                                      "    :warning: " + message % "This")
        return result

    @classmethod
    def get_final_fn(cls, function, message, func_name):
        @wraps(function)
        def fn_wrapper(*args, **kwargs):
            warnings.warn(message % "'{}'".format(func_name))
            return function(*args, **kwargs)

        return fn_wrapper


class Deprecated(Level):
    desc = \
    """
    %s is a deprecated API, use {instead} instead.
    """
    def __new__(cls, instead):
        def decorator(function):
            warn_on_attribute_access(cls, [function], "deprecated")

            kwargs = {cls.msg_params: {"instead": instead}}
            return Level.__new__(cls, function, **kwargs)

        return decorator

deprecated = Deprecated


class Uncommitted(Level):
    desc = \
    """
    %s is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.
    """


uncommitted = Uncommitted


class Volatile(Level):
    desc = \
    """
    %s is a volatile API call that is still in flux and may likely be changed.

    It may also be an inherently private API call that may be exposed, but "YMMV" (your mileage may vary) principles apply.
    """


volatile = Volatile


class Internal(Level):
    desc = \
    """
    %s is an internal API call.

    Components external to Couchbase Python Client should not rely on it is not intended for use outside the module, even to other Couchbase components.
    """

    @classmethod
    def get_final_fn(cls, function, *args):
        return function


internal = Internal


class Committed(Level):
    desc = \
    """
    %s is guaranteed to be supported and remain stable between SDK versions.
    """


committed = Committed


def warn_on_attribute_access(obj, applicable, status):
    """Return a wrapped object that warns about deprecated accesses"""
    applicable = set(applicable)

    class Wrapper(object):
        def __getattr__(self, attr):
            if attr in applicable:
                warnings.warn("Property %s is %s" % (attr, status))

            return getattr(obj, attr)

        def __setattr__(self, attr, value):
            if attr in applicable:
                warnings.warn("Property %s is %s" % (attr, status))
            return setattr(obj, attr, value)
    return Wrapper()
