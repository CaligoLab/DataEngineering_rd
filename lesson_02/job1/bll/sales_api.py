import os
import shutil
from lesson_02.job1.dal import local_disk, sales_api
from lesson_02.constants import BASE_PATH

date = "2022-08-09"
RAW_DIR = os.path.join(BASE_PATH, "data", "raw", "sales", date)

def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    # TODO: implement me
    # 1. get data from the API
    # 2. save data to disk

    # Create/re-create raw_dir
    if not (os.path.exists(raw_dir) and os.path.isdir(raw_dir)):
        os.makedirs(raw_dir, exist_ok=True)
        print("there was no directory, I created")
    else:
        shutil.rmtree(raw_dir, ignore_errors=True)
        os.makedirs(raw_dir)
        print("there was a dir, I deleted and created a new one")

save_sales_to_local_disk(date, RAW_DIR)
    # print("\tI'm in get_sales(...) function!")

