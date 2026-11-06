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

"""
Update Authenticator Command for dynamic credential updates.

This command allows updating the cluster's credentials (password or certificate)
at runtime without reconnecting, enabling testing of mTLS cert refresh functionality.
"""

from __future__ import annotations

import logging
from time import perf_counter_ns
from typing import TYPE_CHECKING, Any

from run import top_level_pb2 as run_pb
from sdk import workload_pb2 as sdk_pb

from .sdk_commands import SdkCommand, SdkCommandResult

if TYPE_CHECKING:
    from couchbase.cluster import Cluster

    from shared import cluster_pb2 as cluster_pb

logger = logging.getLogger(__name__)


class UpdateAuthenticatorCommand(SdkCommand):
    """Command to update cluster authentication credentials at runtime."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the UpdateAuthenticatorCommand.

        Args:
            cluster: The Couchbase cluster instance.
            auth_message: Proto authenticator message with password_auth or certificate_auth.
            initiated: Timestamp when the command was initiated.
            return_result: Whether to return the result.
        """
        self._cluster: Cluster = kwargs['cluster']
        self._auth_message: cluster_pb.Authenticator = kwargs['auth_message']
        self._initiated = kwargs['initiated']
        self._return_result = kwargs.get('return_result')

    @staticmethod
    def create_command(**cmd_kwargs: Any) -> UpdateAuthenticatorCommand:
        """Create and return an UpdateAuthenticatorCommand instance.

        Args:
            cluster: The Couchbase cluster instance.
            auth_message: Proto authenticator message.
            **cmd_kwargs: Common command kwargs (initiated, return_result).

        Returns:
            UpdateAuthenticatorCommand instance.
        """
        return UpdateAuthenticatorCommand(**cmd_kwargs)

    def set_options(self) -> None:
        """Set command options. No additional options for this command."""
        pass

    def execute_command(self) -> run_pb.Result:
        """Execute the update credentials operation.

        Returns:
            run_pb.Result with success or exception.
        """
        # Import lazily to avoid circular import with utils.connections
        from ..utils.connections import ClusterConnectOptions

        try:
            start = perf_counter_ns()
            authenticator = ClusterConnectOptions.create_authenticator(self._auth_message)
            self._cluster.set_authenticator(authenticator)
            end = perf_counter_ns()
            sdk_result = sdk_pb.Result(success=True)
            return run_pb.Result(sdk=sdk_result, elapsedNanos=(end - start), initiated=self._initiated)
        except Exception as e:
            logger.exception("Failed to update authenticator")
            sdk_result = sdk_pb.Result(exception=SdkCommandResult.to_exception(e))
            return run_pb.Result(sdk=sdk_result, initiated=self._initiated)
