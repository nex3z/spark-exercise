#!/usr/bin/env bash
spark-submit \
    --name grpc \
    --master "local[4]" \
    --py-files reader_pb2.py,reader_pb2_grpc.py \
    main.py