import unittest
from lambda_function import lambda_handler  # Ensure filename matches

class TestLambdaFunction(unittest.TestCase):

    def test_lambda_handler_valid_event(self):
        """Test Lambda function with valid input"""
        event = {"data": "Hello"}
        response = lambda_handler(event, None)
        self.assertEqual(response['statusCode'], 200)

    def test_lambda_handler_invalid_event(self):
        """Test Lambda function with invalid input"""
        event = {}  # Empty event to test error handling
        response = lambda_handler(event, None)
        self.assertEqual(response['statusCode'], 400)



import json

def lambda_handler(event, context):
    try:
        print("Received event:", event)  # Debugging statement

        if "data" not in event:
            print("Missing 'data' key!")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'data' key"})
            }

        # Simulate successful processing
        print("Processing data:", event["data"])
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Success", "data": event["data"]})
        }

    except Exception as e:
        print("Error occurred:", str(e))  # Print error details
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error"})
        }


if __name__ == "__main__":
    unittest.main()
