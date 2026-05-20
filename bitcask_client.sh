#!/bin/bash

# BitCask Client Script
# This script interacts with the Central Station API to view BitCask contents

CENTRAL_STATION_URL="http://localhost:8080" # Update this if needed

if [ "$1" == "--view-all" ]; then
    TIMESTAMP=$(date +%s)
    FILE_NAME="${TIMESTAMP}.csv"
    echo "Fetching all keys and saving to ${FILE_NAME}..."
    # TODO: curl endpoint to get all keys and format as CSV
    # curl -s "${CENTRAL_STATION_URL}/api/bitcask/all" > "${FILE_NAME}"
    echo "key,value" > "${FILE_NAME}"
    echo "Done."

elif [ "$1" == "--view" ] && [[ "$2" == --key=* ]]; then
    KEY="${2#*=}"
    echo "Fetching value for key: ${KEY}"
    # TODO: curl endpoint to get value for specific key
    # curl -s "${CENTRAL_STATION_URL}/api/bitcask/key/${KEY}"

elif [ "$1" == "--perf" ] && [[ "$2" == --clients=* ]]; then
    CLIENTS="${2#*=}"
    echo "Running performance test with ${CLIENTS} clients..."
    TIMESTAMP=$(date +%s)
    
    for i in $(seq 1 $CLIENTS); do
        (
            FILE_NAME="${TIMESTAMP}_thread_${i}.csv"
            # TODO: curl endpoint to get all keys and format as CSV
            # curl -s "${CENTRAL_STATION_URL}/api/bitcask/all" > "${FILE_NAME}"
            echo "key,value" > "${FILE_NAME}"
            echo "Thread ${i} finished."
        ) &
    done
    wait
    echo "All threads completed."
else
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view --key=SOME_KEY"
    echo "  ./bitcask_client.sh --perf --clients=100"
fi
