import warnings


def deprecate_module_attribute(mod, deprecated):
    """Return a wrapped object that warns about deprecated accesses"""
    deprecated = set(deprecated)

    class Wrapper(object):
        def __getattr__(self, attr):
            if attr in deprecated:
                warnings.warn("Property %s is deprecated" % attr)

            return getattr(mod, attr)

        def __setattr__(self, attr, value):
            if attr in deprecated:
                warnings.warn("Property %s is deprecated" % attr)
            return setattr(mod, attr, value)
    return Wrapper()