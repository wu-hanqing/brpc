#!/bin/bash

concurrency=$1

export UCX_TLS=rc_mlx5

# ./client --server="10.187.0.6:13339,10.187.0.4:13339,10.187.0.41:13339" \
# ./client --server="10.185.0.25:13339" \
./client --server="10.187.0.4:13339" \
    --use_ucp=true \
    --thread_num=${concurrency} \
    --use_bthread=true \
    --attachment_size=4096

# ./client --server="10.187.0.6:8002,10.187.0.4:8002,10.187.0.41:8002" \
#     --thread_num=${concurrency} \
#     --use_bthread=true

# pubbeta1-nostest1.yq.163.org
