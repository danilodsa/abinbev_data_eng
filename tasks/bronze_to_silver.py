import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os

def bronze_to_silver(bronze_path: str, silver_path: str, timestamp: datetime):
    """
    Transform raw JSON data from the bronze layer into a partitioned Parquet dataset in the silver layer.
    Args:
        bronze_path (str): Path to the raw JSON files in the bronze layer.
        silver_folder_path (str): Path to save the transformed Parquet files in the silver layer.
        timestamp (datetime): Timestamp of the data extraction.
    """
    # Spark session
    try:
        spark = SparkSession.builder \
            .appName("breweriesProcessor") \
            .getOrCreate()
    except Exception as e:
        logging.error(f"Failed to create Spark session: {str(e)}")
        raise

    # Read the raw JSON data from the bronze layer
    file_suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    date_folder = timestamp.strftime("%Y-%m-%d")
    raw_file_path = os.path.join(bronze_path, date_folder, f"breweries_raw_{file_suffix}.json")
    
    raw_breweries_data = spark.read.json(raw_file_path)

    # Select relevant columns for the silver layer
    transformed_df = raw_breweries_data.select(
            col("id"),
            col("name"),
            col("brewery_type"),
            col("street"),
            col("city"),
            col("state_province"),
            col("postal_code"),
            col("country"),
            col("longitude"),
            col("latitude"),
            col("phone"),
            col("website_url")
        )

    # Write the transformed data to the silver layer in Parquet format, partitioned by state
    destination_file_path = os.path.join(silver_path, "breweries.parquet")
    transformed_df.write \
        .mode("append") \
        .partitionBy("state_province") \
        .parquet(destination_file_path)

    logging.info(f"Data successfully transformed and saved to {destination_file_path}")