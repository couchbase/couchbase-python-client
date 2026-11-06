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

__all__ = [
    'MutationState',
]

from typing import TYPE_CHECKING, Any

from couchbase.common.exceptions import MissingTokenException
from couchbase.common.result import MutationToken

if TYPE_CHECKING:
    from couchbase.common.result import MutationResult


class MutationState:
    """A set of mutation tokens a query, search or range scan is to be consistent with.

    Pass it as the ``consistent_with`` option, and the service waits until it has seen every mutation
    in the set before it answers.

    Args:
        *docs: Results of mutations whose tokens to add, as :meth:`add_results` takes them.
        **kwargs: ``quiet``, as :meth:`add_results` takes it.
    """

    def __init__(self, *docs: MutationResult, **kwargs: Any) -> None:
        self._sv: set[MutationToken] = set()
        if docs:
            self.add_results(*docs, **kwargs)

    def add_mutation_token(self, mut_token: MutationToken | None) -> None:
        """Add a mutation token to the set.

        Args:
            mut_token: The token to add.  Anything that is not a :class:`~couchbase.result.MutationToken`
                is ignored.
        """
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)

    def _add_scanvec(self, mut_token: MutationToken) -> bool:
        """Internal method used to specify a scan vector.

        Args:
            mut_token: A tuple in the form of `(vbucket id, vbucket uuid, mutation sequence)`
        """
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)
            return True

        return False

    def add_results(self, *rvs: MutationResult, **kwargs: Any) -> bool:
        """Add the mutation tokens of the given results to the set.

        A result carries a token only if mutation tokens are enabled on the connection, as they are by
        default.

        Args:
            *rvs: One or more results of mutations.
            **kwargs: ``quiet``: when ``True``, a result with no token stops the adding and returns
                ``False`` rather than raising.

        Returns:
            bool: ``True`` if every result's token was added, ``False`` if a result had none and
            ``quiet`` was given.

        Raises:
            :class:`~couchbase.exceptions.MissingTokenException`: If no results are given, or a result
                has no token and ``quiet`` was not given.
        """
        if not rvs:
            raise MissingTokenException(message='No results passed')
        for rv in rvs:
            mut_token = rv.mutation_token()
            if not isinstance(mut_token, MutationToken):
                if kwargs.get('quiet', False) is True:
                    return False
                raise MissingTokenException(
                    message='Result does not contain token')
            if not self._add_scanvec(mut_token):
                return False
        return True

    def add_all(self, bucket: Any, quiet: bool = False) -> bool:
        """Add every mutation made through a bucket to the set.

        Args:
            bucket: The bucket the mutations were made through.
            quiet: When ``True``, a bucket with no mutations returns ``False`` rather than raising.

        Returns:
            bool: ``True`` if at least one mutation was added, ``False`` if none was and ``quiet`` was given.

        Raises:
            :class:`~couchbase.exceptions.MissingTokenException`: If no mutation was added and ``quiet`` was
                not given.
        """
        raise NotImplementedError("Feature currently not implemented in 4.x series of the Python SDK")

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._sv})'
