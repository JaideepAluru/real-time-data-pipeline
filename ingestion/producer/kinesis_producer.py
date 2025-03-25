import boto3
import json
import time
import random
import uuid

# AWS Kinesis client
kinesis_client = boto3.client("kinesis", region_name="us-east-1")

# Kinesis Stream Name
STREAM_NAME = "real-time-data-stream"

# Function to generate sample data
def generate_data():
    return {
        "sensor_id": str(uuid.uuid4()),  # Unique sensor ID
        "temperature": round(random.uniform(20.0, 30.0), 2),  # Simulated temp data
        "humidity": round(random.uniform(50.0, 90.0), 2),  # Simulated humidity data
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")  # Current time
    }

# Function to send data to Kinesis
def send_to_kinesis():
    while True:
        data = generate_data()
        print(f"Sending data: {data}")

        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data),  # Convert dict to JSON
            PartitionKey=data["sensor_id"]  # Ensures balanced distribution
        )
        
        print(f"Record sent! Sequence Number: {response['SequenceNumber']}")
        time.sleep(2)  # Simulate real-time streaming (adjust as needed)

# Run the producer
if __name__ == "__main__":
    send_to_kinesis()
