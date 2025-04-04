import sagemaker
from sagemaker import get_execution_role
from sagemaker.amazon.amazon_estimator import get_image_uri

# Step 1: Set up SageMaker session and role
role = get_execution_role()  # Assumes the role has proper permissions
sagemaker_session = sagemaker.Session()

# Step 2: Define S3 paths for training and validation data
s3_input_train = 's3://your-bucket/preprocessed_data/train/'
s3_input_validation = 's3://your-bucket/preprocessed_data/validation/'

# Step 3: Get the container image for BlazingText (or another algorithm you prefer)
image_uri = get_image_uri(sagemaker_session.boto_session.region_name, 'blazingtext')

# Step 4: Set up the BlazingText Estimator
blazing_text = sagemaker.estimator.Estimator(
    image_uri=image_uri,
    role=role,
    instance_count=1,
    instance_type='ml.p3.2xlarge',
    output_path='s3://your-bucket/output/',
    sagemaker_session=sagemaker_session
)

# Step 5: Define hyperparameters (you can tune these based on your needs)
blazing_text.set_hyperparameters(mode='supervised', epochs=10, vector_dim=300, mini_batch_size=500)

# Step 6: Train the model
blazing_text.fit({'train': s3_input_train, 'validation': s3_input_validation})

# Step 7: Deploy the model
predictor = blazing_text.deploy(instance_type='ml.m4.xlarge', initial_instance_count=1)

# Step 8: Use the model for predictions
predictions = predictor.predict(["I love this product!", "This is terrible."])
print(predictions)

# Step 9: Delete the endpoint when done
predictor.delete_endpoint()
