#!/bin/bash

# ──────────────────────────────────────────────────────────────
# BitCask Client — CLI to interact with the Central Station API
# ──────────────────────────────────────────────────────────────

CENTRAL_STATION_URL="${CENTRAL_STATION_URL:-http://localhost:8080}"

usage() {
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "      Exports all keys/values to a timestamped CSV file."
    echo ""
    echo "  ./bitcask_client.sh --view --key=SOME_KEY"
    echo "      Prints the value associated with a key to stdout."
    echo ""
    echo "  ./bitcask_client.sh --perf --clients=N"
    echo "      Launches N threads, each querying all keys and writing a CSV."
    exit 1
}

fetch_all_to_csv() {
    local file_name="$1"
    echo "key,value" > "${file_name}"
    curl -s "${CENTRAL_STATION_URL}/api/bitcask/all" \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
for k, v in data.items():
    escaped = v.replace('\"', '\"\"')
    print(f'{k},\"{escaped}\"')
" >> "${file_name}"
}

if [ "$1" == "--view-all" ]; then
    TIMESTAMP=$(date +%s)
    FILE_NAME="${TIMESTAMP}.csv"
    echo "Fetching all keys from BitCask..."
    fetch_all_to_csv "${FILE_NAME}"
    echo "Saved to ${FILE_NAME}"

elif [ "$1" == "--view" ] && [[ "$2" == --key=* ]]; then
    KEY="${2#*=}"
    curl -s "${CENTRAL_STATION_URL}/api/bitcask/key/${KEY}"
    echo ""

elif [ "$1" == "--perf" ] && [[ "$2" == --clients=* ]]; then
    CLIENTS="${2#*=}"
    TIMESTAMP=$(date +%s)
    echo "Starting performance test with ${CLIENTS} concurrent clients..."

    for i in $(seq 1 "$CLIENTS"); do
        (
            FILE_NAME="${TIMESTAMP}_thread_${i}.csv"
            fetch_all_to_csv "${FILE_NAME}"
            echo "Thread ${i} done → ${FILE_NAME}"
        ) &
    done

    wait
    echo "All ${CLIENTS} threads completed."

else
    usage
fi
