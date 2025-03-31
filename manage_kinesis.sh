#!/bin/bash

STREAM_NAME="real-time-data-stream"

case "$1" in
  create)
    echo "Creating Kinesis Stream..."
    aws kinesis create-stream --stream-name $STREAM_NAME --shard-count 1
    echo "Stream Created Successfully!"
    ;;
    
  delete)
    echo "Deleting Kinesis Stream..."
    aws kinesis delete-stream --stream-name $STREAM_NAME 
    echo "Stream Deleted Successfully!"
    ;;
    
  *)
    echo "Usage: $0 {create|delete}"
    exit 1
    ;;
esac

