from typing import List, Dict, Any
import requests
from requests import Response
from lesson_02.constants import API_URL, AUTH_TOKEN

date = "2022-08-09"
page = 1

def get_sales(date: str, page: int=1) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: date retrieve the data from
    :return: list of records
    """
    response = requests.get(
        url=API_URL,
        params={'date': date, 'page': page},
        headers={'Authorization': AUTH_TOKEN},
    )
    return response.json()

#get_sales(date, page)

def get_status_code(date: str, page: int=1) -> int:
    """
    Get data from sales API for specified date.

    :param date: date retrieve the data from
    :return: list of records
    """
    response = requests.get(
        url=API_URL,
        params={'date': date, 'page': page},
        headers={'Authorization': AUTH_TOKEN},
    )
    return response.status_code

# status_code = get_status_code(date, page)
# print(status_code)