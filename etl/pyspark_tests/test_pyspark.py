# test_pyspark.py

import pytest
from pyspark.sql import SparkSession
from pyspark_script import clean_data  # Import the transformation function

@pytest.fixture(scope="module")
def spark():
    """Initialize Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .getOrCreate()
    yield spark
    spark.stop()  # Stop the Spark session after the tests

def test_clean_data(spark):
    """Test the clean_data transformation function."""
    data = [("Alice", 30), ("Bob", None), (None, 25), ("Eve", 40)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    cleaned_df = clean_data(df)
    result = cleaned_df.collect()

    # Convert to sets to ignore order issues
    expected_names = {"Alice", "Eve", None}
    result_names = {row["name"] for row in result}

    assert len(result) == 3  # Only 3 rows should remain
    assert result_names == expected_names  # Ensure the right names are present

