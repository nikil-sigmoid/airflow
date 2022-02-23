from datetime import datetime
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def get_params() -> Dict[str, Any]:
    context = get_current_context()
    params = context["params"]
    return params



@dag(
    dag_id="get_current_context_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    params={"my_param": "param_value"},
)
def my_pipeline():
    name = ""
    @task()
    def get_data():
        params = get_params()
        name = params
        print("first function")
        print(params)

    @task
    def get_data1(pa_ok):
        print("second function")
        print(name)
        print(pa_ok)

    get_data1(get_data())


# print(name)
pipeline = my_pipeline()
print(pipeline)
# print(name)