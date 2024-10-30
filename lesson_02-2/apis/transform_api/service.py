import json
import os
from fastavro import writer, parse_schema
import pandas as pd

from apis.schemas.avro_schemas import PURCHASE_SCHEMA


parsed_schema = parse_schema(PURCHASE_SCHEMA)


def convert_json_to_avro(json_directory, avro_directory):
    """Convert all files from JSON directory to AVRO with same names."""
    # Check if directory exists
    if not os.path.exists(avro_directory):
        os.makedirs(avro_directory)

    # Transform all files
    for file_name in os.listdir(json_directory):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(json_directory, file_name)
            avro_file_path = os.path.join(avro_directory, file_name.replace('.json', '.avro'))

            # Read JSON
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                records = json.load(json_file)

            # Write AVRO
            with open(avro_file_path, 'wb') as avro_file:
                writer(avro_file, parsed_schema, records)


def convert_json_to_csv(json_directory, csv_directory):
    """Convert all files from JSON directory to CSV."""
    # Check if directory exists
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    # Transform all files
    for file_name in os.listdir(json_directory):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(json_directory, file_name)
            csv_file_path = os.path.join(csv_directory, file_name.replace('.json', '.csv'))

            # Read JSON
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                records = json.load(json_file)

            # Collect data as DF
            df = pd.DataFrame(records)

            # Write CSV
            df.to_csv(csv_file_path, index=False)
