import os
import shutil
from lesson_02.job1.dal import local_disk, sales_api
from lesson_02.common_funcs import create_dir
from lesson_02.constants import BASE_PATH

date = "2022-08-09"
RAW_DIR = os.path.join(BASE_PATH, "lesson_02", "data", "raw", "sales", date)

def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    # TODO: implement me
    # 1. get data from the API
    # 2. save data to disk
    create_dir(date, RAW_DIR)


save_sales_to_local_disk(date, RAW_DIR)
    # print("\tI'm in get_sales(...) function!")

