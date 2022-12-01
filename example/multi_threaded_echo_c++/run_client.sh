#!/bin/bash

concurrency=1

export UCX_TLS=rc_mlx5

./client --server="10.187.0.91:13339" \
    --use_ucp=true \
    --thread_num=${concurrency} \
    --use_bthread=true \
    --attachment_size=4096 \
    --defer_close_second=1
