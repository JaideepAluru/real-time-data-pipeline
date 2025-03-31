import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # <-- Import DynamicFrame here

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

# Convert to DataFrame and perform transformations
df = dynamic_frame.toDF()
df = df.dropna()  # Removing null values as a basic transformation

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
