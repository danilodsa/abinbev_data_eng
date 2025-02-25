import pytest
from unittest.mock import Mock, patch
from tasks.data_ingestion import breweriesAPIExtractor, ingest_breweries_data

SAMPLE_BREWERY = {
    "id": "test1",
    "name": "Test Brewery 1",
    "brewery_type": "micro",
    "street": "123 Test St",
    "city": "Test City",
    "state": "Test State",
    "postal_code": "12345",
    "country": "United States",
    "longitude": "-123.4567",
    "latitude": "45.6789",
    "phone": "1234567890",
    "website_url": "http://test1.com"
}

@pytest.fixture
def mock_response():
    mock = Mock()
    mock.json.return_value = [SAMPLE_BREWERY]
    mock.raise_for_status.return_value = None
    return mock

@pytest.fixture
def extractor():
    return breweriesAPIExtractor()

@patch('requests.get')
def test_fetch_breweries_success(mock_get, mock_response, extractor):
    mock_get.return_value = mock_response

    result = extractor.fetch_breweries(page=1)

    assert result == [SAMPLE_BREWERY]
    mock_get.assert_called_once_with(
        f"{extractor.base_url}/breweries",
        params={"page": 1, "per_page": 50}
    )

def test_ingest_breweries_data_error_handling():
    """Test error handling in ingest_breweries_data"""
    with patch('tasks.data_ingestion.breweriesAPIExtractor') as mock_extractor_class:
        mock_extractor = Mock()
        mock_extractor_class.return_value = mock_extractor
        mock_extractor.fetch_all_breweries.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            ingest_breweries_data("/tmp")