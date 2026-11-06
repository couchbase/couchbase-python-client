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
