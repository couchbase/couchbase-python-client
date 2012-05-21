import warnings

def setup_warning_catcher():
    """Wrap warnings.showwarning with code that records warnings.
    From: http://stackoverflow.com/questions/2324820/count-warnings-in-python-2-4
    """

    caught_warnings = []
    original_showwarning = warnings.showwarning

    def custom_showwarning(*args,  **kwargs):
        caught_warnings.append(args[0])
        return original_showwarning(*args, **kwargs)

    warnings.showwarning = custom_showwarning
    return caught_warnings
