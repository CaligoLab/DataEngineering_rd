import json
import logging
import os
import shutil

import httpx

from constants import URL, AUTH_TOKEN

logger = logging.getLogger(__name__)
client = httpx.AsyncClient()


def save_raw_data(raw_dir: str, date: str, data_type: str = "sales") -> None:
    """Collect data using API from all pages and save them into files."""
    # Create or Re-create RAW dir
    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir, exist_ok=True)
    else:
        shutil.rmtree(raw_dir, ignore_errors=True)
        os.makedirs(raw_dir)

    # Save the data
    page = 1
    while True:
        file_path = f"{os.path.abspath(raw_dir)}/{data_type}_{date}_{page}.json"
        try:
            data = get_data_from_api(date, page)
        except AssertionError as e:
            logger.error(e)
            break
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
            logger.info(f"Saved {file_path}")
        page = page + 1


def get_data_from_api(date: str, page: int = 1) -> dict:
    with httpx.Client() as client:
        response = client.get(URL, params={"date": date, "page": page}, headers={"Authorization": AUTH_TOKEN})
        assert response.status_code == 200, response.status_code
    return response.json()
