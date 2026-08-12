import logging
from concurrent.futures import ThreadPoolExecutor

import grpc

from .generated import performer_pb2_grpc as performer_pb_grpc
from .performer_service import PerformerServiceServicer


def serve():
    port = '8060'
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    servicer = PerformerServiceServicer()
    performer_pb_grpc.add_PerformerServiceServicer_to_server(servicer, server)
    server.add_insecure_port('[::]:8060')
    server.start()
    logging.getLogger().info("Server started, listening on " + port)
    server.wait_for_termination()


def main():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    serve()


if __name__ == "__main__":
    main()
