#!/usr/bin/env bash
spark-submit \
    --name py-files \
    --master local[4] \
    --py-files extra/util.py \
    main.py