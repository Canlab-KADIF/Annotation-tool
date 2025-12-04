#!/bin/bash

# Enhanced pipeline that uses upload serial number for better tracking

# Check arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <dataset_name> <dataset_type> <folder_path>"
    echo "Example: $0 my_dataset LIDAR_FUSION /path/to/data"
    exit 1
fi

DATASET_NAME=$1
DATASET_TYPE=$2
FOLDER_PATH=$3

echo "=== Step 1: Creating Dataset ==="
CREATE_OUTPUT=$(./create_dataset.sh "$DATASET_NAME" "$DATASET_TYPE")
echo "$CREATE_OUTPUT"

# Extract Dataset ID
DATASET_ID=$(echo "$CREATE_OUTPUT" | grep "CREATED_DATASET_ID=" | cut -d'=' -f2)

if [ -z "$DATASET_ID" ] || [ "$DATASET_ID" == "null" ]; then
    echo "Error: Failed to capture Dataset ID."
    exit 1
fi

echo "Captured Dataset ID: $DATASET_ID"

echo "=== Step 2: Uploading Data ==="
UPLOAD_OUTPUT=$(./upload_data.sh "$FOLDER_PATH" "$DATASET_ID" "$DATASET_NAME")
echo "$UPLOAD_OUTPUT"

# Extract Serial Number
SERIAL_NUMBER=$(echo "$UPLOAD_OUTPUT" | grep "UPLOAD_SERIAL_NUMBER=" | cut -d'=' -f2)

if [ -z "$SERIAL_NUMBER" ] || [ "$SERIAL_NUMBER" == "null" ]; then
    echo "Error: Failed to capture Upload Serial Number."
    exit 1
fi

echo "Captured Serial Number: $SERIAL_NUMBER"

echo "=== Step 2.5: Waiting for Import ==="
./wait_for_import.sh "$DATASET_ID" "$SERIAL_NUMBER"

if [ $? -ne 0 ]; then
    echo "Error: Import failed or timed out."
    exit 1
fi

echo "=== Step 3: Running Model ==="
# Default Model ID is 1, can be parameterized if needed
MODEL_ID=1
./run_model.sh "$DATASET_ID" "$MODEL_ID"
