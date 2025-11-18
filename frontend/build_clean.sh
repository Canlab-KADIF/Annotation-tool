#!/bin/bash

# 빌드
docker build -t gkes-merge-frontend:latest .

# Get all dangling image IDs
dangling_images=$(docker images -f "dangling=true" -q)

for img in $dangling_images; do
  echo "Processing image: $img"

  # Stop and remove containers using this image
  containers=$(docker ps -a -q --filter ancestor=$img)
  if [ ! -z "$containers" ]; then
    echo "Removing containers using image $img..."
    docker rm -f $containers
  fi

  # Remove the image
  echo "Removing image $img..."
  docker rmi -f $img
done

echo "Cleanup complete."