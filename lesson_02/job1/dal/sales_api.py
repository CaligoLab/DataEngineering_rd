from typing import List, Dict, Any
import requests
from requests import Response
from lesson_02.constants import API_URL, AUTH_TOKEN

# date = "2022-08-09"
# page = 1

def get_sales(date: str, page: int=1) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: date retrieve the data from
    :return: list of records
    """

    # dummy return:
    return [
        {
            "client": "Tara King",
            "purchase_date": "2022-08-09",
            "product": "Phone",
            "price": 1062
        },
        {
            "client": "Lauren Hawkins",
            "purchase_date": "2022-08-09",
            "product": "TV",
            "price": 1373
        },
        # ...
    ]
