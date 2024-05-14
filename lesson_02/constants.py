import os
from dotenv import find_dotenv, load_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
BASE_PATH = os.getenv("BASE_PATH")