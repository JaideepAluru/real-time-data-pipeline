import boto3
import json
import time

# AWS Kinesis client
kinesis_client = boto3.client("kinesis", region_name="us-east-1")

# Stream name
STREAM_NAME = "real-time-data-stream"

def get_shard_iterator():
    """Fetch the shard iterator for reading records"""
    response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

    iterator_response = kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )
    return iterator_response["ShardIterator"]

def read_from_kinesis():
    """Read and process records from Kinesis"""
    shard_iterator = get_shard_iterator()
    
    while True:
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        shard_iterator = response["NextShardIterator"]  # Get the next iterator
        
        for record in response["Records"]:
            data = json.loads(record["Data"])
            print(f"Received Data: {data}")
        
        time.sleep(2)  # Control polling frequency

# Run the consumer
if __name__ == "__main__":
    read_from_kinesis()
