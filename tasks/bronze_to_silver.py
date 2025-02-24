import logging
from datetime import datetime
import pandas as pd
import os

def bronze_to_silver(bronze_path: str, silver_path: str, timestamp: datetime):
    """
    Transform raw JSON data from the bronze layer into a partitioned Parquet dataset in the silver layer.
    Args:
        bronze_path (str): Path to the raw JSON files in the bronze layer.
        silver_folder_path (str): Path to save the transformed Parquet files in the silver layer.
        timestamp (datetime): Timestamp of the data extraction.
    """


    # Read the raw JSON data from the bronze layer
    date_folder = timestamp.strftime("%Y-%m-%d")
    raw_file_path = os.path.join(bronze_path, date_folder, f"breweries_raw.json")
    
    raw_breweries_data = pd.read_json(raw_file_path)

    # Select relevant columns for the silver layer
    cleaned_df = raw_breweries_data[[
        "id",
        "name",
        "brewery_type",
        "street",
        "city",
        "state_province",
        "postal_code",
        "country", 
        "longitude",
        "latitude",
        "phone",
        "website_url"
    ]]

    # Write the transformed data to the silver layer in Parquet format, partitioned by state
    destination_file_path = os.path.join(silver_path, "breweries.parquet")
    
    cleaned_df.to_parquet(destination_file_path, partition_cols=["state_province"])

    logging.info(f"Data successfully transformed and saved to {destination_file_path}")