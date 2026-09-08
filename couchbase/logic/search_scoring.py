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

from abc import ABC, abstractmethod
from typing import (Any,
                    Dict,
                    Optional)

from couchbase.exceptions import InvalidArgumentException


class SearchScoring(ABC):
    """Base class for search scoring modes.

    For hybrid search, score fusion combines the FTS and vector results into one ranked list.

    Available modes:

        :class:`~couchbase.search_scoring.ReciprocalRankFusion`: merges by rank.

        :class:`~couchbase.search_scoring.RelativeScoreFusion`: merges by normalized score.

        :class:`~couchbase.search_scoring.ScoringNone`: disables scoring.

    """

    @abstractmethod
    def __init__(self, strategy: str, **params: Optional[int]) -> None:
        # Keep these values in sync with search_scoring_mode in bindings.yaml.
        self._params: Dict[str, Any] = {'scoring_type': strategy}
        for name, value in params.items():
            if value is None:
                continue
            # bool is an int subclass, but is not valid for these parameters.
            if not isinstance(value, int) or isinstance(value, bool):
                raise InvalidArgumentException(message=f'Expected {name} to be of type int.')
            # The C++ fields are uint32_t and do not report a failed conversion.
            if not 0 <= value <= 0xFFFFFFFF:
                raise InvalidArgumentException(message=f'Expected {name} to be in the range 0 to 4294967295.')
            self._params[name] = value

    def as_encodable(self) -> Dict[str, Any]:
        """
            **INTERNAL**
        """
        return self._params


class ReciprocalRankFusion(SearchScoring):
    """**UNCOMMITTED** This API may change in the future.

    Combines FTS and vector results by rank.

    Args:
        rank_constant (int, optional): The rank constant of the Reciprocal Rank Fusion formula.
        window_size (int, optional): How many results per list are considered for fusion.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If either parameter is not an int in the
            range 0 to 4294967295.

    """

    def __init__(self,
                 rank_constant=None,  # type: Optional[int]
                 window_size=None,  # type: Optional[int]
                 ) -> None:
        super().__init__('reciprocal_rank_fusion', rank_constant=rank_constant, window_size=window_size)


class RelativeScoreFusion(SearchScoring):
    """**UNCOMMITTED** This API may change in the future.

    Combines FTS and vector results by normalized score.

    Args:
        window_size (int, optional): How many results per list are considered for fusion.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If window_size is not an int in the
            range 0 to 4294967295.

    """

    def __init__(self,
                 window_size=None,  # type: Optional[int]
                 ) -> None:
        super().__init__('relative_score_fusion', window_size=window_size)


class ScoringNone(SearchScoring):
    """Disables result scoring.

    This is equivalent to the deprecated ``disable_scoring`` option.

    """

    def __init__(self) -> None:
        super().__init__('none')
