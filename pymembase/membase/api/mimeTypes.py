"""
    Copyright (C) 2008 Benjamin O'Steen

    This file is part of python-fedoracommons.

    python-fedoracommons is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    python-fedoracommons is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with python-fedoracommons.  If not, see <http://www.gnu.org/licenses/>.
"""

__license__ = 'GPL http://www.gnu.org/licenses/gpl.txt'
__author__ = "Benjamin O'Steen <bosteen@gmail.com>"
__version__ = '0.1'

class mimeTypes(object):
    def getDictionary(self):
        mimetype_to_extension = {}
        extension_to_mimetype = {}
        mimetype_to_extension['text/plain'] = 'txt'
        mimetype_to_extension['text/xml'] = 'xml'
        mimetype_to_extension['text/css'] = 'css'
        mimetype_to_extension['text/javascript'] = 'js'
        mimetype_to_extension['text/rtf'] = 'rtf'
        mimetype_to_extension['text/calendar'] = 'ics'
        mimetype_to_extension['application/msword'] = 'doc'
        mimetype_to_extension['application/msexcel'] = 'xls'
        mimetype_to_extension['application/x-msword'] = 'doc'
        mimetype_to_extension['application/vnd.ms-excel'] = 'xls'
        mimetype_to_extension['application/vnd.ms-powerpoint'] = 'ppt'
        mimetype_to_extension['application/pdf'] = 'pdf'
        mimetype_to_extension['text/comma-separated-values'] = 'csv'

        mimetype_to_extension['image/jpeg'] = 'jpg'
        mimetype_to_extension['image/gif'] = 'gif'
        mimetype_to_extension['image/jpg'] = 'jpg'
        mimetype_to_extension['image/tiff'] = 'tiff'
        mimetype_to_extension['image/png'] = 'png'

        # And hacky reverse lookups
        for mimetype in mimetype_to_extension:
            extension_to_mimetype[mimetype_to_extension[mimetype]] = mimetype

        mimetype_extension_mapping = {}
        mimetype_extension_mapping.update(mimetype_to_extension)
        mimetype_extension_mapping.update(extension_to_mimetype)

        return mimetype_extension_mapping
