#!/bin/bash

# Source shared configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it first."
    exit 1
fi

# Check arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <dataset_id> <model_id>"
    echo "Example: $0 12 1"
    exit 1
fi

DATASET_ID=$1
MODEL_ID=$2
BASE_URL="${BASE_URL}"
USERNAME="${USERNAME}"
PASSWORD="${PASSWORD}"

# 1. Login to get token
echo "Logging in as $USERNAME..."
LOGIN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/login" \
  -H "Content-Type: application/json" \
  -d "{ \"username\": \"$USERNAME\", \"password\": \"$PASSWORD\" }")

TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.data.token')

if [ "$TOKEN" == "null" ]; then
    echo "Login failed. Attempting to register..."
    REGISTER_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/register" \
      -H "Content-Type: application/json" \
      -d "{ \"username\": \"$USERNAME\", \"password\": \"$PASSWORD\" }")
    TOKEN=$(echo "$REGISTER_RESPONSE" | jq -r '.data.token')
    
    if [ "$TOKEN" == "null" ]; then
        echo "Registration failed. Response: $REGISTER_RESPONSE"
        exit 1
    fi
    echo "Registration successful."
else
    echo "Login successful."
fi

echo "Token obtained."

# 2. Trigger Model Run
echo "Triggering model run for Dataset ID: $DATASET_ID, Model ID: $MODEL_ID..."

# Construct JSON payload
# Based on ModelRunDTO:
# {
#   "datasetId": 12,
#   "modelId": 1,
#   "dataFilterParam": {
#     "dataCountRatio": 100,
#     "isExcludeModelData": false
#   },
#   "resultFilterParam": {}
# }

PAYLOAD=$(jq -n \
                  --argjson datasetId "$DATASET_ID" \
                  --argjson modelId "$MODEL_ID" \
                  '{
                    datasetId: $datasetId,
                    modelId: $modelId,
                    dataFilterParam: {
                      dataCountRatio: 100,
                      isExcludeModelData: false
                    },
                    resultFilterParam: null
                  }')

echo "Payload:"
echo "$PAYLOAD"

RUN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/model/modelRun" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "$PAYLOAD")

echo "Model Run Response:"
echo "$RUN_RESPONSE" | jq .

# Check for success (assuming 200 OK and empty body or specific success code)
# The controller returns void, so likely just 200 OK.
# If there is an error, it usually returns a JSON with code != OK.

CODE=$(echo "$RUN_RESPONSE" | jq -r '.code // "OK"') 
# Note: If response is empty (void), jq might fail or return null. 
# But Spring Boot usually returns a wrapper if configured, or just 200.
# Let's assume standard API wrapper based on other endpoints.

if [ "$CODE" == "OK" ] || [ -z "$RUN_RESPONSE" ]; then
    echo "Model run triggered successfully."
else
    echo "Error triggering model run."
    exit 1
fi
