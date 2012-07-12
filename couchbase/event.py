#
# Copyright 2012, Couchbase, Inc.
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

from time import sleep as _sleep, time as _time

class Stopwatch:
    """Timeout checker with very low polling rate (~ once per second)
    """
    def __init__(self, timeout):
        self.__iterations = 0
        self.__state = True
        self.__start_time = _time()
        self.__timeout = timeout

    def __iadd__(self, other):
        self.__iterations += other
        if self.__iterations > 5000:
            if _time() - self.__start_time > self.__timeout:
                self.__state = False
            self.__iterations = 0
        return self

    def check(self):
        return self.__state


class Event:
    """Simplified implementation of threading.Event
    """
    def __init__(self):
        self.__state = False

    def set(self):
        self.__state = True

    def isSet(self):
        return self.__state

    def is_set(self):
        return self.__state

    def wait(self, timeout=30.0):
        DELAY = 0.0001 # 0.1 ms

        stopwatch = Stopwatch(timeout=timeout)

        while not self.__state and stopwatch.check():
            _sleep(DELAY)
            stopwatch += 1
