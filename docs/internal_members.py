#  Copyright 2016-2026. Couchbase, Inc.
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

"""Leave out of the reference every member whose docstring opens with ``**INTERNAL**``.

The SDK marks a method, property or attribute that its own code calls, and an application should not,
with ``**INTERNAL**`` at the start of its docstring.  It stays where it is, and the reference does not
describe it.  A public name cannot be marked: ``python -m tools.docgen check`` fails on one.
"""

from __future__ import annotations

import enum
import inspect
from typing import Any

INTERNAL = '**INTERNAL**'


def is_internal(obj: object) -> bool:
    """Whether ``obj`` is marked, by the docstring autodoc would show for it.

    That is the one an override with no docstring of its own inherits, as autodoc does.
    """
    # An enum member's docstring is its class's.
    if isinstance(obj, enum.Enum):
        return False
    doc = inspect.getdoc(obj)
    return doc is not None and doc.startswith(INTERNAL)


def _skip_member(app: Any, what: str, name: str, obj: object, skip: bool, options: Any) -> bool | None:
    return True if is_internal(obj) else None


def setup(app: Any) -> dict[str, Any]:
    app.connect('autodoc-skip-member', _skip_member)
    return {'parallel_read_safe': True}
