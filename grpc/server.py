import time
from concurrent import futures

import grpc
import reader_pb2
import reader_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Reader(reader_pb2_grpc.ReaderServicer):

    def ReadFile(self, request, context):
        filename = request.filename
        print("ReadFile(): Reading {}".format(filename))
        with open(filename, 'r') as file:
            data = file.read()
        return reader_pb2.ReadReply(content=data)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reader_pb2_grpc.add_ReaderServicer_to_server(Reader(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
