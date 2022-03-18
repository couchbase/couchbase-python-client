from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List)

from couchbase.exceptions import MissingTokenException
from couchbase.result import MutationToken

if TYPE_CHECKING:
    from couchbase.result import MutationResult


class MutationState:
    def __init__(self, *docs,  # type: List[MutationResult]
                 **kwargs  # type: Dict[str, Any]
                 ):
        self._sv = set()
        if docs:
            self.add_results(*docs, **kwargs)

    def add_mutation_token(self, mut_token  # type: MutationToken
                           ) -> None:
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)

    def _add_scanvec(self, mut_token  # type: MutationToken
                     ) -> bool:
        """
        Internal method used to specify a scan vector.
        :param mut_token: A tuple in the form of
            `(vbucket id, vbucket uuid, mutation sequence)`
        """
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)
            return True

        return False

    def add_results(self, *rvs,  # type: List[MutationResult]
                    **kwargs  # type: Dict[str, Any]
                    ) -> bool:
        """
        Changes the state to reflect the mutation which yielded the given
        result.

        In order to use the result, the `enable_mutation_tokens` option must
        have been specified in the connection string, _and_ the result
        must have been successful.

        :param rvs: One or more :class:`~.OperationResult` which have been
            returned from mutations
        :param quiet: Suppress errors if one of the results does not
            contain a convertible state.
        :return: `True` if the result was valid and added, `False` if not
            added (and `quiet` was specified
        :raise: :exc:`~.MissingTokenException` if `result` does not contain
            a valid token
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

    def add_all(self, bucket, quiet=False):
        """
        Ensures the query result is consistent with all prior
        mutations performed by a given bucket.

        Using this function is equivalent to keeping track of all
        mutations performed by the given bucket, and passing them to
        :meth:`~add_result`

        :param bucket: A :class:`~couchbase_core.client.Client` object
            used for the mutations
        :param quiet: If the bucket contains no valid mutations, this
            option suppresses throwing exceptions.
        :return: `True` if at least one mutation was added, `False` if none
            were added (and `quiet` was specified)
        :raise: :exc:`~.MissingTokenException` if no mutations were added and
            `quiet` was not specified
        """
        raise NotImplementedError("Feature currently not implemented in 4.x series of the Python SDK")

    def __repr__(self):
        return "MutationState:{}".format(self._token)
