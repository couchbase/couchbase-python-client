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

from __future__ import annotations

from typing import Any


def untyped_kwargs(**kwargs: Any) -> dict[str, Any]:
    """Keywords a test passes on purpose outside what the method declares.

    A hidden alias, a private hook or a value of the wrong type is a runtime path worth testing
    and a type error at the call.  Splatting what this returns keeps the rest of the call checked.
    """
    return kwargs


# `skip_connect` is a private hook the cluster impl pops, so a test can build a cluster that
# never opens a connection.
SKIP_CONNECT = untyped_kwargs(skip_connect='TEST_SKIP_CONNECT')
