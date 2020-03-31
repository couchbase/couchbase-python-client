#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import couchbase_core._libcouchbase as C
from collections import defaultdict
from string import Template
import json

from couchbase_core import CompatibilityEnum
from couchbase_core.supportability import uncommitted, volatile
from typing import *
import inspect
import re
import sys
from boltons.funcutils import wraps
try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict


