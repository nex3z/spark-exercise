#!/usr/bin/env bash
spark-submit \
    --name py-files \
    --master local[4] \
    --py-files extra.zip \
    main_with_zip.py