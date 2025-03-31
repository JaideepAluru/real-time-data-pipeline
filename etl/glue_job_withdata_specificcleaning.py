import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define source and destination S3 paths
SOURCE_S3_PATH = "s3://real-time-data-storage-jaideep/"
DEST_S3_PATH = "s3://real-time-data-destination-storage/"

# Read data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [SOURCE_S3_PATH]},
    format="json"
)

# Convert to DataFrame for validation
df = dynamic_frame.toDF()

# Data Validation:
# 1. Ensure 'column_name' is not null (basic non-null validation)
df = df.filter(col('column_name').isNotNull())  # Replace 'column_name' with actual field names

# 2. Ensure 'column_name' is of type integer (data type validation)
df = df.filter(df['column_name'].cast("int").isNotNull())  # Replace with actual column and desired type

# 3. Add additional validation checks as needed
# Example: Ensure that 'age' is greater than 0 and 'salary' is greater than 5000
df = df.filter(df['age'] > 0)  # Replace 'age' with actual column and condition
df = df.filter(df['salary'] > 5000)  # Replace 'salary' with actual column and condition

# Data Cleaning: Remove rows with null values
df = df.dropna()

# Convert back to Glue DynamicFrame
cleaned_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "cleaned_dynamic_frame")

# Write processed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_frame,
    connection_type="s3",
    connection_options={"path": DEST_S3_PATH},
    format="parquet"
)

job.commit()
