"""
This file contains the controller that accepts command via HTTP
and triggers business logic layer
"""
import os
from flask import Flask, render_template, request
from lesson_02.constants import AUTH_TOKEN, BASE_PATH
from lesson_02.job1.bll.sales_api import save_sales_to_local_disk

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main():
    # -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    triggers business logic layer

    Proposed POST body in JSON:
    {
      "date: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    # if request.method == 'GET':
    #     return render_template('index.html')
    # elif request.method == 'POST':
    #     date: str = request.form.get('date')
    #     if not date:
    #         return {
    #             "message": "date parameter missed",
    #         }, 400
    #
    #     raw_dir: str = os.path.join(BASE_PATH, "lesson_02", "data", "raw", "sales", date)
    #     input_data = save_sales_to_local_disk(date=date, raw_dir=raw_dir)
    #     return input_data

    input_data: dict = request.json
    # TO DO: implement me
    # request.headers['Content-Type'] = 'application/json'
    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')
    if not date:
        return {
            "message": "date parameter missed",
        }, 400
    if not raw_dir:
        return {
            "message": "path parameter missed",
        }, 400
    full_path: str = os.path.join(BASE_PATH, raw_dir, date)
    result = save_sales_to_local_disk(date=date, raw_dir=full_path)

    return result

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)