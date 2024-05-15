from typing import List, Dict, Any
import requests
from lesson_02.constants import API_URL, AUTH_TOKEN


def get_sales(date: str, page: int = 1) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param page:
    :param date: date retrieve the data from
    :return: list of records
    """
    response = requests.get(
        url=API_URL,
        params={'date': date, 'page': page},
        headers={'Authorization': AUTH_TOKEN},
    )
    return response.json()


def get_status_code(date: str, page: int = 1) -> int:
    """
    Get status code from sales API for specified date.

    :param page:
    :param date: date retrieve the data from
    :return: status code
    """
    response = requests.get(
        url=API_URL,
        params={'date': date, 'page': page},
        headers={'Authorization': AUTH_TOKEN},
    )
    return response.status_code
