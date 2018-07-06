#!/usr/bin/env bash
spark-submit \
    --name "mysql" \
    --master "local" \
    --packages mysql:mysql-connector-java:5.1.46 \
    main_mysql.py