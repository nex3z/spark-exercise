from pyspark import SparkConf, SparkContext

import grpc
import reader_pb2
import reader_pb2_grpc


def main():
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    channel = grpc.insecure_channel('localhost:50051')
    stub = reader_pb2_grpc.ReaderStub(channel)
    response = stub.ReadFile(reader_pb2.ReadRequest(filename='../data/98-0.txt'))

    rdd_data = sc.parallelize([response.content])
    rdd_lines = rdd_data.flatMap(lambda x: x.split('\n'))
    print("lines = {}".format(rdd_lines.count()))

    sc.stop()


if __name__ == '__main__':
    main()
