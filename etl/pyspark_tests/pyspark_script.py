# pyspark_script.py

from pyspark.sql import functions as F

def clean_data(df):
    """Remove rows where the 'age' column is null."""
    return df.filter(F.col("age").isNotNull())

