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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from couchbase.encryption import Key


class Keyring(ABC):
    @abstractmethod
    def get_key(self,
                key_id,  # type: str
                ) -> Key:
        """Returns requested key

        Args:
            keyid (str): Key ID to retrieve

        Returns:
            :class:`~couchbase.encryption.Key`: The corresponding :class:`~couchbase.encryption.Key`
            of the provided key_id.

        Raises:
            :raises :class:`~couchbase.exceptions.CryptoKeyNotFoundException`
        """
