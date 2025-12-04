#!/bin/bash

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it first."
    exit 1
fi

# Check arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <folder_path> <dataset_id> [dataset_name]"
    exit 1
fi

FOLDER_PATH=$1
DATASET_ID=$2
DATASET_NAME=${3:-$(basename "$FOLDER_PATH")}

# Source shared configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

BASE_URL="${BASE_URL}"
USERNAME="${USERNAME}"
PASSWORD="${PASSWORD}"

# Debug output
echo "DEBUG: Input path: $FOLDER_PATH"
echo "DEBUG: Dataset ID: $DATASET_ID"
echo "DEBUG: Dataset name: $DATASET_NAME"

# Check if we are in Local Upload Mode
MOUNTED_PATH="${MOUNTED_DATA_PATH}"
IS_LOCAL_MODE=false

if [[ "$FOLDER_PATH" == "$MOUNTED_PATH"* ]]; then
    IS_LOCAL_MODE=true
    echo "DEBUG: Local Upload Mode detected."
fi

# 1. Handle Archive Creation or Selection
IS_INPUT_FILE=false
if [ -f "$FOLDER_PATH" ]; then
    echo "Input is a file. Using it directly."
    TAR_NAME="$FOLDER_PATH"
    IS_INPUT_FILE=true
else
    # Input is a directory, create archive
    if [ "$IS_LOCAL_MODE" = true ]; then
        # Create tar in the parent directory of the folder
        PARENT_DIR=$(dirname "$FOLDER_PATH")
        TAR_NAME="${PARENT_DIR}/${DATASET_NAME}.tar"
        echo "Creating tar archive '$FOLDER_PATH' to '$TAR_NAME'..."
        tar -cvf "$TAR_NAME" -C "$PARENT_DIR" "$(basename "$FOLDER_PATH")"
    else
        TAR_NAME="${DATASET_NAME}.tar"
        echo "Creating tar archive '$FOLDER_PATH' to '$TAR_NAME'..."
        tar -cvf "$TAR_NAME" -C "$(dirname "$FOLDER_PATH")" "$(basename "$FOLDER_PATH")"
    fi

    if [ $? -ne 0 ]; then
        echo "Error: Failed to create tar archive."
        exit 1
    fi
fi

# 2. Login to get token
echo "Logging in as $USERNAME..."
LOGIN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/login" \
  -H "Content-Type: application/json" \
  -d "{ \"username\": \"$USERNAME\", \"password\": \"$PASSWORD\" }")

TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.data.token')

if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
    echo "Login failed. Attempting to register..."
    REGISTER_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/register" \
      -H "Content-Type: application/json" \
      -d "{ \"username\": \"$USERNAME\", \"password\": \"$PASSWORD\" }")
    TOKEN=$(echo "$REGISTER_RESPONSE" | jq -r '.data.token')
    
    if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
        echo "Registration failed. Response: $REGISTER_RESPONSE"
        exit 1
    fi
    echo "Registration successful."
else
    echo "Login successful."
fi

echo "Token obtained."

if [ "$IS_LOCAL_MODE" = true ]; then
    # Local Mode: Skip Presigned URL and MinIO Upload
    echo "Skipping MinIO upload (Local Mode)."
    ACCESS_URL="$TAR_NAME"
    SOURCE_TYPE="LOCAL"
else
    # Standard Mode: Upload to MinIO
    # 3. Generate Presigned URL
    echo "Generating upload URL..."
    PRESIGNED_RESPONSE=$(curl -s -X GET "$BASE_URL/api/data/generatePresignedUrl?fileName=$(basename "$TAR_NAME")&datasetId=$DATASET_ID" \
      -H "Authorization: Bearer $TOKEN")

    PRESIGNED_URL=$(echo "$PRESIGNED_RESPONSE" | jq -r '.data.presignedUrl')
    ACCESS_URL=$(echo "$PRESIGNED_RESPONSE" | jq -r '.data.accessUrl')

    if [ "$PRESIGNED_URL" == "null" ]; then
        echo "Failed to generate presigned URL. Response: $PRESIGNED_RESPONSE"
        exit 1
    fi

    # 4. Upload to MinIO
    echo "Uploading tar file to MinIO..."
    curl -X PUT "$PRESIGNED_URL" -T "$TAR_NAME"

    if [ $? -ne 0 ]; then
        echo "Error: Failed to upload file to MinIO."
        exit 1
    fi
    echo "Upload to MinIO successful."
    SOURCE_TYPE="LOCAL" 
fi

# 5. Trigger Import
echo "Triggering import..."
UPLOAD_RESPONSE=$(curl -s -X POST "$BASE_URL/api/data/upload" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{
    \"fileUrl\": \"$ACCESS_URL\",
    \"datasetId\": $DATASET_ID,
    \"source\": \"$SOURCE_TYPE\",
    \"resultType\": null,
    \"dataFormat\": null
  }")

echo "Import Response:"
echo "$UPLOAD_RESPONSE" | jq .

# Extract serial number for tracking
SERIAL_NUMBER=$(echo "$UPLOAD_RESPONSE" | jq -r '.data')
echo "UPLOAD_SERIAL_NUMBER=$SERIAL_NUMBER"

# Cleanup
if [ "$IS_LOCAL_MODE" = false ] && [ "$IS_INPUT_FILE" = false ]; then
    rm "$TAR_NAME"
fi
echo "Done."

