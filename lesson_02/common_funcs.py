import os
import shutil

def create_dir(date: str, dir_path: str) -> None:
    # Create/re-create directory
    if not (os.path.exists(dir_path) and os.path.isdir(dir_path)):
        os.makedirs(dir_path, exist_ok=True)
        print("there was no directory, I created")
    else:
        shutil.rmtree(dir_path, ignore_errors=True)
        os.makedirs(dir_path)
        print("there was a dir, I deleted and created a new one")