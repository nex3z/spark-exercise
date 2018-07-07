#!/usr/bin/env bash
#!/usr/bin/env bash
spark-submit \
    --name grpc \
    --master "local" \
    --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
    main_graph_frame.py