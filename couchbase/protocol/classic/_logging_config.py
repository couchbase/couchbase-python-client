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

"""**INTERNAL** The C++ core's logging, registered with `couchbase.common._logging` when the binding loads."""

import atexit
import logging

from couchbase.common._logging import (LOGGER_NAME,
                                       EnvLogging,
                                       register_core_log_backend)
from couchbase.protocol.classic._pycbc_core import pycbc_logger, shutdown_logger
from couchbase.protocol.classic._pycbc_core.core_metadata import get_metadata


class ClassicLogBackend:
    """**INTERNAL** Drives the C++ core's logger on behalf of `couchbase.common._logging`."""

    def __init__(self) -> None:
        self._logger = pycbc_logger()

    def apply_env(self, env: EnvLogging) -> None:
        if env.filename is not None:
            self._logger.create_logger(level=env.level,
                                       filename=env.filename,
                                       enable_console=int(env.enable_console))
        else:
            self._logger.create_logger(level=env.level)
        logging.getLogger(LOGGER_NAME).debug(get_metadata(as_str=True))

    def route_to(self, logger: logging.Logger, level: int) -> None:
        self._logger.configure_logging_sink(logger, level)
        logger.debug(get_metadata(as_str=True))

    def enable_protocol_logger(self, filename: str) -> None:
        self._logger.enable_protocol_logger(filename)

    def teardown(self) -> None:
        # if using a console logger we let the natural course of shutdown happen, if using Python logging
        # we need a cleaner mechanism to shutdown the C++ logger prior to the Python interpreter starting to finalize
        if not (self._logger.is_console_logger() or self._logger.is_file_logger()):
            # Deactivate the sink before dropping our reference to it. A C++ core bug in
            # logger::shutdown() (get_file_logger() is reset by value, so the global copy
            # survives) means shutdown_logger() doesn't actually release the sink, which can
            # then outlive us and destruct at static teardown, after Py_Finalize, where its
            # PyGILState_Ensure() would be fatal.
            self._logger.shutdown_sink()
            shutdown_logger()
            del self._logger


_BACKEND = ClassicLogBackend()
atexit.register(_BACKEND.teardown)
register_core_log_backend(_BACKEND)
