import grpc

import reader_pb2
import reader_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = reader_pb2_grpc.ReaderStub(channel)
    response = stub.ReadFile(reader_pb2.ReadRequest(filename='../data/98-0.txt'))
    print("content = {}".format(response.content))


if __name__ == '__main__':
    run()
