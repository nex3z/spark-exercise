#!/usr/bin/env bash
spark-submit \
    --name upload-files \
    --master "local[4]" \
    --files ../data/98-0.txt \
    main.py