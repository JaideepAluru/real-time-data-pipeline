from flask import Flask, request, jsonify
import sagemaker
import boto3

# Initialize Flask app
app = Flask(__name__)

# Setup AWS SageMaker runtime
runtime = boto3.client('runtime.sagemaker')

# Endpoint name (make sure to replace with your deployed model endpoint)
endpoint_name = 'your-endpoint-name'

# Define the prediction function
def predict_sentiment(text):
    response = runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='text/csv',  # For text data
        Body=text
    )
    result = response['Body'].read().decode('utf-8')
    return result

@app.route('/predict', methods=['POST'])
def predict():
    # Get text data from the incoming request
    data = request.get_json()
    text = data.get('text')

    if not text:
        return jsonify({'error': 'No text provided'}), 400

    # Make sentiment prediction
    prediction = predict_sentiment(text)
    
    # Return the prediction as a JSON response
    return jsonify({'prediction': prediction})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
