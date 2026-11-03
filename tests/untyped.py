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
