#!/bin/bash

# Source shared configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# Configuration
BASE_URL="${BASE_URL}"
USERNAME="${USERNAME}"
PASSWORD="${PASSWORD}"
DATASET_NAME=${1:-"test_dataset_$(date +%s)"}
DATASET_TYPE=${2:-"LIDAR_FUSION"}

# 1. Login
echo "Logging in as $USERNAME..."
LOGIN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

# Parse token
TOKEN=$(echo $LOGIN_RESPONSE | jq -r '.data.token')

if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
  echo "Login failed. Attempting to register..."
  
  # 2. Register (if login failed, maybe user doesn't exist)
  REGISTER_RESPONSE=$(curl -s -X POST "$BASE_URL/api/user/register" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")
    
  TOKEN=$(echo $REGISTER_RESPONSE | jq -r '.data.token')
  
  if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
    echo "Registration failed. Response:"
    echo $REGISTER_RESPONSE
    echo "Login Response:"
    echo $LOGIN_RESPONSE
    exit 1
  fi
  echo "Registration successful."
else
  echo "Login successful."
fi

echo "Token obtained."

# 3. Create Dataset
echo "Creating dataset '$DATASET_NAME'..."
CREATE_RESPONSE=$(curl -s -X POST "$BASE_URL/api/dataset/create" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{ \"name\": \"$DATASET_NAME\", \"type\": \"$DATASET_TYPE\" }")

echo "Response:"
echo "$CREATE_RESPONSE" | jq .

# Extract and print ID for pipeline usage
DATASET_ID=$(echo "$CREATE_RESPONSE" | jq -r '.data.id')
echo "CREATED_DATASET_ID=$DATASET_ID"
