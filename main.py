from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.data_ingestion import ingest_breweries_data
from tasks.bronze_to_silver import bronze_to_silver
from tasks.silver_to_gold import silver_to_gold

#Local tests only
if __name__ == "__main__":
    timestamp=datetime.now()
    ingest_breweries_data(output_path="./data/bronze/breweries/json/", timestamp=timestamp)
    bronze_to_silver(bronze_path="./data/bronze/breweries/json/", silver_path="./data/silver/breweries/", timestamp=timestamp)
    silver_to_gold(silver_path="./data/silver/breweries/breweries.parquet", gold_path="./data/gold/breweries/")