#!/bin/bash

service_id_list="
{service_id_1}
{service_id_2}
...
"

function execute_make {
    local target=$1
    IFS=$'\n' read -r -d '' -a service_ids <<< "$service_id_list"
    local total=${#service_ids[@]}
    for i in "${!service_ids[@]}"; do
        local service_id=${service_ids[$i]}
        echo "Processing $((i + 1)) / $total: SERVICE_ID=$service_id"
        make "$target" "SERVICE_ID=$service_id"
    done
}

if [ $# -eq 0 ]; then
    echo "Usage: $0 <function_name>"
    exit 1
fi

case "$1" in
    export-dynamodb-to-s3)
        execute_make export-dynamodb-to-s3
        ;;
    transfer-to-gcs)
        execute_make transfer-to-gcs
        ;;
    bulk-write)
        execute_make bulk-write
        ;;
    streaming-write)
        execute_make streaming-write
        ;;
    drain-job)
        execute_make drain-job
        ;;
    close-pubsub)
        execute_make close-pubsub
        ;;
    *)
        echo "Invalid function name: $1"
        exit 1
        ;;
esac
