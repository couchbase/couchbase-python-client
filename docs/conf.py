# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

import os
import sys

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.dirname(__file__))

# -- Project information -----------------------------------------------------

project = 'Couchbase Python Client Library'
copyright = '2022, Couchbase, Inc.'
author = 'Couchbase, Inc.'

# from .. import couchbase_version
import couchbase_version  # nopep8 # isort:skip # noqa: E402

try:
    from datetime import datetime
    year = f'{datetime.today():%Y}'
except BaseException:
    year = "2022"
copyright = "2013-{}, Couchbase, Inc.".format(year)

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The full version, including alpha/beta/rc tags.
sdk_version = couchbase_version.get_version()
version = sdk_version
release = sdk_version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx_rtd_theme',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.extlinks',
    'sphinx_copybutton',
    'enum_tools.autoenum'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
# html_theme = 'classic'

html_theme_options = {
    'display_version': True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
html_static_path = []

extlinks = {
    'analytics_intro': ('https://docs.couchbase.com/server/current/analytics/introduction.html', None),
    'search_create_idx': ('https://docs.couchbase.com/server/current/fts/fts-creating-indexes.html', None)
}
