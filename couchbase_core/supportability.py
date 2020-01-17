import warnings

from boltons.funcutils import wraps


def deprecate_module_attribute(mod, deprecated=[]):
    return warn_on_attribute_access(mod, deprecated, "deprecated")


class Level(object):
    @classmethod
    def wrap(cls, function):
        """
        Mark a function as uncommitted

        :param function: input function
        :return: marked function
        """
        message = cls.__doc__+"\n"

        func_name = getattr(function, '__qualname__', function.__name__)

        result = cls.get_final_fn(function, message, func_name)
        result.__doc__ = (function.__doc__+"\n\n" if function.__doc__ else "") + "    :warning: " + message % "This"
        return result

    @classmethod
    def get_final_fn(cls, function, message, func_name):
        @wraps(function)
        def fn_wrapper(*args, **kwargs):
            warnings.warn(message % "'{}'".format(func_name))
            return function(*args, **kwargs)

        return fn_wrapper


class Uncommitted(Level):
    """
    %s is an uncommitted API call that is unlikely to change, but may still change as final consensus on its behavior has not yet been reached.
    """


uncommitted = Uncommitted.wrap


class Volatile(Level):
    """
    %s is a volatile API call that is still in flux and may likely be changed.

    It may also be an inherently private API call that may be exposed, but "YMMV" (your mileage may vary) principles apply.
    """


volatile = Volatile.wrap


class Internal(Level):
    """
    %s is an internal API call.

    Components external to Couchbase Python Client should not rely on it is not intended for use outside the module, even to other Couchbase components.
    """

    @classmethod
    def get_final_fn(cls, function, *args):
        return function


internal = Internal.wrap


class Committed(Level):
    """
    %s is guaranteed to be supported and remain stable between SDK versions.
    """


committed = Committed.wrap


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