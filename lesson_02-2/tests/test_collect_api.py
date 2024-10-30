import os
import shutil
from unittest import mock
from unittest.mock import patch, mock_open, MagicMock

import httpx
import pytest

from apis.collect_api.service import save_raw_data, get_data_from_api
from constants import URL, AUTH_TOKEN


@pytest.mark.parametrize("exists", [True, False])
def test_save_raw_data(exists):
    raw_dir = "test_raw_dir"
    date = "2024-04-04"
    data_type = "sales"
    sample_data = {"test": "data"}
    mock_open_func = mock_open()

    with patch("os.path.exists", return_value=exists), \
            patch("os.makedirs"), \
            patch("shutil.rmtree"), \
            patch("builtins.open", mock_open_func), \
            patch("json.dump") as mock_json_dump, \
            patch("lesson_02.apis.collect_api.service.get_data_from_api",
                  return_value=sample_data) as mock_get_data_from_api:
        save_raw_data(raw_dir, date, data_type)

        # Check directory clean
        assert os.makedirs.called
        if exists:
            assert shutil.rmtree.called

        # Check call get_data_from_api
        assert mock_get_data_from_api.called
        assert mock_json_dump.called

        mock_json_dump.assert_called_with(sample_data, mock_open_func.return_value, indent=4)


def test_get_data_from_api_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'key': 'value'}

    # Patch 'httpx.Client.get' to return the mock_response
    with patch('httpx.Client.get', return_value=mock_response):
        response_data = get_data_from_api('2024-01-01', 1)

        # Assert that the response data matches the expected data
        assert response_data == {'key': 'value'}, "The response data does not match the expected value."

        # Ensure the get method was called with the correct parameters
        httpx.Client.get.assert_called_once_with(
            URL,
            params={'date': '2024-01-01', 'page': 1},
            headers={'Authorization': AUTH_TOKEN}
        )


@pytest.fixture
def mock_httpx_client():
    with mock.patch('httpx.Client') as MockClient:
        # Create a mock response object with the desired attributes
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'key': 'value'}

        # Configure the mock client to return the mock response when its 'get' method is called
        mock_client_instance = MockClient.return_value
        mock_client_instance.get.return_value = mock_response
        yield mock_client_instance


def test_get_data_from_api_failure(mock_httpx_client):
    mock_response = mock_httpx_client.get.return_value.__enter__.return_value
    mock_response.status_code = 404

    with pytest.raises(AssertionError):
        get_data_from_api('2024-01-01', 1)
