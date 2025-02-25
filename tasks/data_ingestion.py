from datetime import datetime
import os
import requests
from typing import List, Dict
import logging
import json

class breweriesAPIExtractor:
    def __init__(self, base_url: str = "https://api.openbrewerydb.org"):
        self.base_url = base_url
        self.per_page = 50

    def fetch_all_breweries(self) -> List[Dict]:
        """Fetch all breweries from the API using pagination"""
        all_breweries = []
        page = 1

        logging.info("Starting to ingest breweries data")
        while True:
            try:
                breweries = self.fetch_breweries(page=page)
                if not breweries:
                    break
                
                all_breweries.extend(breweries)
                logging.info(f"Fetched page {page}. Total: {len(all_breweries)}")
                
                page += 1
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching page {page}: {str(e)}")
                raise

        return all_breweries


    def fetch_breweries(self, page: int = 1) -> List[Dict]:
        """Fetch a single page of breweries"""
        try:
            response = requests.get(
                f"{self.base_url}/breweries",
                params={"page": page, "per_page": self.per_page}
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching breweries: {str(e)}")
            raise


def ingest_breweries_data(output_path: str, timestamp: datetime):
    """
    Extract data from the breweries API and save it to the bronze layer.
    Args:
        output_path (str): Path to save the raw JSON files in the bronze layer.
        timestamp (datetime): Timestamp of the data extraction.
    """
    try:
        extractor = breweriesAPIExtractor()
        data = extractor.fetch_all_breweries()
        
        # Save to bronze
        date_folder = timestamp.strftime("%Y-%m-%d")
        directory_path = os.path.join(output_path, date_folder)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        file_path = os.path.join(directory_path, f"breweries_raw.json")

        with open(file_path, 'w') as f:
            json.dump(data, f)
        
        logging.info(f"Raw data saved to {file_path}")
                
    except Exception as e:
        logging.error(f"Failed to fetch and save breweries data: {str(e)}")
        raise
  