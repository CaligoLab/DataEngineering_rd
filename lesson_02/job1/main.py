"""
This file contains the controller that accepts command via HTTP
and triggers business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.constants import AUTH_TOKEN, API_URL
from lesson_02.job1.bll.sales_api import save_sales_to_local_disk


if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    triggers business logic layer

    Proposed POST body in JSON:
    {
      "date: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    # TODO: implement me
    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')

    if not date:
        return {
            "message": "date parameter missed",
        }, 400

    save_sales_to_local_disk(date=date, raw_dir=raw_dir)

    return {
               "message": "Data retrieved successfully from API",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
