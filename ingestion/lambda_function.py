import json
import boto3
import base64
import os

# Initialize S3 client
s3_client = boto3.client('s3')

# Get the S3 bucket name from environment variables
S3_BUCKET = os.getenv('S3_BUCKET', 'your-bucket-name')

def lambda_handler(event, context):
    """
    AWS Lambda function to process data from Kinesis and store in S3.
    Includes error handling for missing environment variables, invalid records, and S3 failures.
    """
    if not S3_BUCKET:
        print("❌ ERROR: S3_BUCKET environment variable is missing.")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET environment variable is not set'})
        }

    records = []
    
    try:
        for record in event['Records']:
            try:
                # Decode and parse data
                payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                records.append(json.loads(payload))
            except Exception as e:
                print(f"⚠️ Skipping invalid record: {e}")
                continue  # Skip invalid records
        
        if not records:
            print("⚠️ No valid records found. Nothing to store.")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'No valid records to process'})
            }

        # Convert records to JSON string
        file_content = json.dumps(records, indent=2)
        s3_key = f"raw_data/{context.aws_request_id}.json"

        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content
        )

        print(f"✅ Data successfully stored in S3: {s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data stored successfully', 's3_key': s3_key})
        }

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
import json

