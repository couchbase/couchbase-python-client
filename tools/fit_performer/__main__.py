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

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import grpc

import performer_pb2_grpc as performer_pb_grpc

from .performer_service import PerformerServiceServicer


def serve() -> None:
    port = '8060'
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    # The service stub declares every RPC abstract.  The class protoc writes does not: each one the
    # performer leaves out answers UNIMPLEMENTED.  So it is built through its type, which mypy lets an
    # abstract class be called through, rather than by name.
    servicer = cast('type[PerformerServiceServicer]', PerformerServiceServicer)()
    performer_pb_grpc.add_PerformerServiceServicer_to_server(servicer, server)
    server.add_insecure_port('[::]:8060')
    server.start()
    logging.getLogger().info("Server started, listening on " + port)
    server.wait_for_termination()


def main() -> None:
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    serve()


if __name__ == "__main__":
    main()
