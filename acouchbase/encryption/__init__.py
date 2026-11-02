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

"""The `acouchbase.encryption` import path.

The same objects as `couchbase.encryption`, re-exported so asyncio code imports them from its own
package.
"""

__all__ = [
    'CryptoManager',
    'Decrypter',
    'Encrypter',
    'EncryptionResult',
    'Key',
    'Keyring',
]

from acouchbase.encryption.crypto_manager import CryptoManager  # noqa: F401
from acouchbase.encryption.decrypter import Decrypter  # noqa: F401
from acouchbase.encryption.encrypter import Encrypter  # noqa: F401
from acouchbase.encryption.encryption_result import EncryptionResult  # noqa: F401
from acouchbase.encryption.key import Key  # noqa: F401
from acouchbase.encryption.keyring import Keyring  # noqa: F401
