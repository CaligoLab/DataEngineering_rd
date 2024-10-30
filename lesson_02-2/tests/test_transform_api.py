import pytest
from unittest.mock import patch, mock_open

from apis.transform_api.service import convert_json_to_avro


@pytest.fixture
def mock_os_operations():
    with patch('os.path.exists') as mock_exists, \
            patch('os.makedirs') as mock_makedirs, \
            patch('os.listdir') as mock_listdir:
        yield mock_exists, mock_makedirs, mock_listdir


@pytest.fixture
def mock_file_operations():
    with patch('builtins.open', mock_open(
            read_data='{"client": "John Doe", "purchase_date": "2024-01-01", "product": "Laptop", "price": 1000}')) as mock_file, \
            patch('lesson_02.apis.transform_api.service.writer') as mock_writer:
        yield mock_file, mock_writer


def test_directory_creation(mock_os_operations, mock_file_operations):
    mock_exists, mock_makedirs, _ = mock_os_operations
    mock_exists.return_value = False

    convert_json_to_avro('json_dir', 'avro_dir')

    mock_makedirs.assert_called_once_with('avro_dir')


def test_json_to_avro_conversion(mock_os_operations, mock_file_operations):
    _, _, mock_listdir = mock_os_operations
    mock_listdir.return_value = ['test.json']

    convert_json_to_avro('json_dir', 'avro_dir')

    # Check if avro writer was called
    mock_file_operations[1].assert_called()
    # Check if the json file was opened
    assert mock_file_operations[0].call_args_list[0][0][0].endswith(
        'json_dir/test.json')
    # Check if the avro file was opened for writing
    assert mock_file_operations[0].call_args_list[1][0][0].endswith(
        'avro_dir/test.avro')


def test_non_json_files_skipped(mock_os_operations, mock_file_operations):
    _, _, mock_listdir = mock_os_operations
    mock_listdir.return_value = ['test.txt', 'another.json']

    convert_json_to_avro('json_dir', 'avro_dir')
    # Avro writer should be called only once for the .json file
    assert mock_file_operations[1].call_count == 1
