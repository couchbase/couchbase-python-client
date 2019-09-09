import warnings

from boltons.funcutils import wraps


def deprecate_module_attribute(mod, deprecated=[]):
    return warn_on_attribute_access(mod, deprecated, "deprecated")


def uncommitted(function):
    """
    Mark a function as uncommitted
    :param function: input function
    :return: marked function
    """
    message = "%s is an uncommitted API call which may be subject to change in future.\n"

    func_name = getattr(function, '__qualname__', function.__name__)

    @wraps(function)
    def uncommitted_wrapper(*args, **kwargs):
        warnings.warn(message % "'{}'".format(func_name))
        return function(*args, **kwargs)

    uncommitted_wrapper.__doc__ = (function.__doc__+"\n\n" if function.__doc__ else "") + "    :warning: " + message % "This"

    return uncommitted_wrapper


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