import pytest
import pandas as pd
import os
from unittest.mock import patch, Mock
from tasks.silver_to_gold import silver_to_gold

# Sample test data
SAMPLE_SILVER_DATA = [
    {
        "brewery_type": "micro",
        "city": "Cincinnati",
        "state_province": "Ohio",
        "id": "1"
    },
    {
        "brewery_type": "brewpub",
        "city": "Cincinnati",
        "state_province": "Ohio",
        "id": "2"
    },
    {
        "brewery_type": "micro",
        "city": "Cleveland",
        "state_province": "Ohio",
        "id": "3"
    },
    {
        "brewery_type": "micro",
        "city": "Detroit",
        "state_province": "Michigan",
        "id": "4"
    },
    # Duplicate record for testing deduplication
    {
        "brewery_type": "micro",
        "city": "Detroit",
        "state_province": "Michigan",
        "id": "4"
    }
]

@pytest.fixture
def sample_silver_data(tmp_path):
    silver_df = pd.DataFrame(SAMPLE_SILVER_DATA)
    silver_path = tmp_path / "silver" / "breweries.parquet"
    silver_path.parent.mkdir(parents=True)
    silver_df.to_parquet(str(silver_path))
    return silver_path

@pytest.fixture
def gold_path(tmp_path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir(parents=True)
    return gold_dir

def test_silver_to_gold_successful_transformation(sample_silver_data, gold_path):
    # Execute processing
    silver_to_gold(str(sample_silver_data), str(gold_path))

    # Check if both parquet files were created
    city_path = os.path.join(str(gold_path), "breweries_per_city.parquet")
    state_path = os.path.join(str(gold_path), "breweries_per_state.parquet")

    assert os.path.exists(city_path)
    assert os.path.exists(state_path)

    # Read and verify city-level aggregations
    city_df = pd.read_parquet(city_path)
    assert len(city_df) > 0
    assert all(col in city_df.columns for col in ["brewery_type", "city", "state_province", "brewery_count"])

    # Verify deduplication (Detroit should have count 1, not 2)
    detroit_count = city_df[city_df["city"] == "Detroit"]["brewery_count"].iloc[0]
    assert detroit_count == 1

    # Read and verify state-level aggregations
    state_df = pd.read_parquet(state_path)
    assert len(state_df) > 0
    assert all(col in state_df.columns for col in ["brewery_type", "state_province", "brewery_count"])

    # Verify aggregation counts
    ohio_micro_count = state_df[
        (state_df["state_province"] == "Ohio") & 
        (state_df["brewery_type"] == "micro")
    ]["brewery_count"].iloc[0]
    assert ohio_micro_count == 2


def test_silver_to_gold_missing_columns(tmp_path):
    # Create data with missing columns
    invalid_data = pd.DataFrame([{"id": "1", "name": "Test"}])
    silver_path = tmp_path / "silver" / "breweries.parquet"
    silver_path.parent.mkdir(parents=True)
    invalid_data.to_parquet(str(silver_path))

    with pytest.raises(Exception):
        silver_to_gold(str(silver_path), str(tmp_path / "gold"))


def test_silver_to_gold_missing_silver_file():
    with pytest.raises(Exception):
        silver_to_gold(
            "/nonexistent/path/breweries.parquet",
            "/some/gold/path"
        )