import os
import json
from lesson_02.job1.dal import local_disk, sales_api
from lesson_02.common_funcs import create_dir
from lesson_02.constants import BASE_PATH

# date = "2022-08-09"
# RAW_DIR  = os.path.join(BASE_PATH, "lesson_02", "data", "raw", "sales", date)

def save_sales_to_local_disk(date: str, raw_dir: str, data_type: str = "sales"):
    # TODO: implement me
    # 1. get data from the API
    # 2. save data to disk

    """Collect data using API from all pages and save them into files."""

    # Create/re-create directory
    create_dir(date, raw_dir)

    # Save pages into files
    page = 1
    status_code = sales_api.get_status_code(date, page)
    while status_code == 200:
        file_path = f"{os.path.abspath(raw_dir)}\{data_type}_{date}_{page}.json"
        data = sales_api.get_sales(date, page)

        local_disk.save_to_disk(data, file_path)
        print(f"\tSaved page {page} with save_sales_to_local_disk")
        page = page + 1
        status_code = sales_api.get_status_code(date, page)

    return {
               "message": "Data retrieved successfully from API",
           }, 201

