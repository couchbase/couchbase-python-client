import logging
import warnings

lcb_min_version_baseline = (2, 9, 0)


def get_lcb_min_version():
    result = lcb_min_version_baseline
    try:
        # check the version listed in README.rst isn't greater than lcb_min_version
        # bump it up to the specified version if it is
        import docutils.parsers.rst
        import docutils.utils
        import docutils.frontend

        parser = docutils.parsers.rst.Parser()

        with open("README.rst") as README:
            settings = docutils.frontend.OptionParser().get_default_values()
            settings.update(
                dict(tab_width=4, report_level=1, pep_references=False, rfc_references=False, syntax_highlight=False),
                docutils.frontend.OptionParser())
            document = docutils.utils.new_document(README.name, settings=settings)

            parser.parse(README.read(), document)
            readme_min_version = tuple(
                map(int, document.substitution_defs.get("libcouchbase_version").astext().split('.')))
            result = max(result, readme_min_version)
            logging.info("min version is {}".format(result))
    except Exception as e:
        warnings.warn("problem: {}".format(e))
    return result