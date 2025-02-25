import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os

"""
Transform raw JSON data from the bronze layer into a partitioned Parquet dataset in the silver layer.
"""
parser = argparse.ArgumentParser()
parser.add_argument("--timestamp", required=True)
args = parser.parse_args()
timestamp = args.timestamp

# Spark session
try:
    spark = SparkSession.builder \
        .appName("breweriesProcessor") \
        .getOrCreate()
except Exception as e:
    logging.error(f"Failed to create Spark session: {str(e)}")
    raise

# Read the raw JSON data from the bronze layer
# file_suffix = timestamp.strftime("%Y%m%d_%H%M%S")
raw_file_path = os.path.join("./data/bronze/breweries/json/", timestamp, f"breweries_raw.json")

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
destination_file_path = os.path.join("./data/silver/breweries/", "breweries.parquet")
transformed_df.write \
    .mode("append") \
    .partitionBy("state_province") \
    .parquet(destination_file_path)

logging.info(f"Data successfully transformed and saved to {destination_file_path}")

spark.stop()