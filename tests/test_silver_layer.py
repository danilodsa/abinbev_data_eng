import pytest
import pandas as pd
import os
from datetime import datetime
from unittest.mock import patch, Mock
from tasks.bronze_to_silver import bronze_to_silver

SAMPLE_BREWERIES_DATA = [
    {
        "id": "brewery-1",
        "name": "Test Brewery 1",
        "brewery_type": "micro",
        "street": "123 Test St",
        "city": "Test City",
        "state_province": "Ohio",
        "postal_code": "12345",
        "country": "United States",
        "longitude": "-83.1234",
        "latitude": "39.1234",
        "phone": "1234567890",
        "website_url": "http://testbrewery1.com",
        "extra_field": "should_be_removed"
    },
    {
        "id": "brewery-2",
        "name": "Test Brewery 2",
        "brewery_type": "brewpub",
        "street": "456 Test Ave",
        "city": "Test Town",
        "state_province": "Michigan",
        "postal_code": "67890",
        "country": "United States",
        "longitude": "-84.5678",
        "latitude": "42.5678",
        "phone": "0987654321",
        "website_url": "http://testbrewery2.com",
        "extra_field": "should_be_removed"
    }
]

@pytest.fixture
def sample_bronze_data(tmp_path):
    date_folder = datetime.now().strftime("%Y-%m-%d")
    bronze_folder = tmp_path / date_folder
    bronze_folder.mkdir(parents=True)

    # Create sample JSON file
    json_path = bronze_folder / "breweries_raw.json"
    pd.DataFrame(SAMPLE_BREWERIES_DATA).to_json(json_path)

    return tmp_path

@pytest.fixture
def silver_path(tmp_path):
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    return silver_dir

def test_bronze_to_silver_successful_transformation(sample_bronze_data, silver_path):
    timestamp = datetime.now()

    bronze_to_silver(str(sample_bronze_data), str(silver_path), timestamp)

    parquet_base_path = os.path.join(str(silver_path), "breweries.parquet")
    assert os.path.exists(parquet_base_path)

    df = pd.read_parquet(parquet_base_path)

    assert len(df) == 2  # Should have two rows
    assert "extra_field" not in df.columns  # Extra field should be removed
    assert set(df["state_province"].unique()) == {"Ohio", "Michigan"}  # Check partitioning

    # Verify all required columns are present
    required_columns = [
        "id", "name", "brewery_type", "street", "city", "state_province",
        "postal_code", "country", "longitude", "latitude", "phone", "website_url"
    ]
    assert all(col in df.columns for col in required_columns)


def test_bronze_to_silver_invalid_json(tmp_path):
    # Create invalid JSON file
    date_folder = datetime.now().strftime("%Y-%m-%d")
    bronze_folder = tmp_path / date_folder
    bronze_folder.mkdir(parents=True)

    with open(bronze_folder / "breweries_raw.json", "w") as f:
        f.write("invalid json data")

    with pytest.raises(Exception):
        bronze_to_silver(
            str(tmp_path),
            str(tmp_path / "silver"),
            datetime.now()
    )