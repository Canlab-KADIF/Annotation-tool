#!/bin/bash

# Enhanced version that checks upload record status for better accuracy
# This version tracks the actual upload job status and progress

# Source shared configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# Check arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <dataset_id> <serial_number>"
    echo "  dataset_id: The dataset ID"
    echo "  serial_number: The upload serial number returned from upload API"
    exit 1
fi

DATASET_ID=$1
SERIAL_NUMBER=$2
BASE_URL="${BASE_URL}"
USERNAME="${USERNAME}"
PASSWORD="${PASSWORD}"
MAX_RETRIES=500
SLEEP_SECONDS=2

# 1. Login to get token
LOGIN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/login" \
  -H "Content-Type: application/json" \
  -d "{ \"username\": \"$USERNAME\", \"password\": \"$PASSWORD\" }")

TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.data.token')

if [ "$TOKEN" == "null" ]; then
    echo "Login failed."
    exit 1
fi

echo "Waiting for data import to complete..."
echo "Dataset ID: $DATASET_ID"
echo "Serial Number: $SERIAL_NUMBER"
echo ""

for ((i=1; i<=MAX_RETRIES; i++)); do
    # Query upload record by serial number
    UPLOAD_RESPONSE=$(curl -s -X GET "$BASE_URL/api/data/findUploadRecordBySerialNumbers?serialNumbers=$SERIAL_NUMBER" \
      -H "Authorization: Bearer $TOKEN")
    
    # Extract upload record data
    STATUS=$(echo "$UPLOAD_RESPONSE" | jq -r '.data[0].status // "UNKNOWN"')
    TOTAL_DATA_NUM=$(echo "$UPLOAD_RESPONSE" | jq -r '.data[0].totalDataNum // 0')
    PARSED_DATA_NUM=$(echo "$UPLOAD_RESPONSE" | jq -r '.data[0].parsedDataNum // 0')
    ERROR_MESSAGE=$(echo "$UPLOAD_RESPONSE" | jq -r '.data[0].errorMessage // ""')
    
    echo "Attempt $i/$MAX_RETRIES:"
    echo "  Status: $STATUS"
    echo "  Progress: $PARSED_DATA_NUM / $TOTAL_DATA_NUM"
    
    # Check if import failed
    if [ "$STATUS" == "FAILED" ]; then
        echo ""
        echo "✗ Import failed!"
        if [ -n "$ERROR_MESSAGE" ]; then
            echo "  Error: $ERROR_MESSAGE"
        fi
        exit 1
    fi
    
    # Check if all data is parsed (most reliable indicator)
    if [ "$TOTAL_DATA_NUM" -gt 0 ] && [ "$PARSED_DATA_NUM" -eq "$TOTAL_DATA_NUM" ]; then
        echo ""
        echo "✓ Import completed successfully!"
        echo "  Total items imported: $TOTAL_DATA_NUM"
        exit 0
    fi
    
    # Still processing
    echo "  Still processing..."
    
    sleep $SLEEP_SECONDS
done

echo ""
echo "Timeout waiting for data import after $((MAX_RETRIES * SLEEP_SECONDS)) seconds."
exit 1
