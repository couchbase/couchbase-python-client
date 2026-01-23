#  Copyright 2016-2022. Couchbase, Inc.
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

from functools import wraps
from typing import (Any,
                    Callable,
                    Coroutine,
                    Dict,
                    List,
                    Optional)

from acouchbase.management.logic.overload_registry import AsyncOverloadRegistry

mgmt_registry = AsyncOverloadRegistry()


class AsyncOverloadManager:

    @classmethod
    def handle_overload(cls,
                        is_default_overload: Optional[bool] = None,
                        overload_types: Optional[List[str]] = None,
                        issue_warning: Optional[bool] = False):
        def decorator(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
            if is_default_overload is not None:
                if fn.__qualname__ in mgmt_registry:
                    mgmt_registry.add_overload(fn,
                                               is_default=is_default_overload,
                                               overload_types=overload_types)
                else:
                    mgmt_registry.register_method(fn,
                                                  is_default=is_default_overload,
                                                  overload_types=overload_types,
                                                  issue_warning_if_deprecated=issue_warning)

            @wraps(fn)
            async def wrapped_fn(self, *args: Any, **kwargs: Dict[str, Any]):
                func = mgmt_registry.get(fn.__qualname__, fn)
                return await func(self, *args, **kwargs)
            return wrapped_fn
        return decorator
