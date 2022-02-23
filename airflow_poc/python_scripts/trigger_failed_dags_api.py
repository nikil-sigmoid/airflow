import itertools
from datetime import datetime as dt, timedelta
import requests

DAG_ID = "catchup_backfill"

URL = f"http://192.168.1.36:8080/api/v1/dags/{DAG_ID}/dagRuns"
# URL = f"http://localhost:8080/api/v1/dags/{DAG_ID}/dagRuns"

USER = "your_airflow_username"
PASS = "your_airflow_password"
LOGICAL_DATE = "logical_date"
PARAMS = {"start_date_lte": "2022-01-05T14:38:01.707791+00:00", "start_date_gte": "2022-01-01T12:38:01.707791+00:00"}


def get_failed_logical_dates():
    # data = requests.get(url=URL, auth=(USER, PASS), params=PARAMS)
    data = requests.get(url=URL, auth=(USER, PASS))
    return [dag_run["logical_date"] for dag_run in data.json()['dag_runs'] if dag_run["state"] == "failed"]


def pick_latest_logical_for_a_day(logical_time_list):
    return [list(g)[0] for k, g in itertools.groupby(logical_time_list, lambda x: x[0:11])]


def add_one_sec_logical_time(logical_time_to_be_exec):
    return [str(dt.fromisoformat(tm) + timedelta(milliseconds=1)) for tm in logical_time_to_be_exec]


def trigger_failed_tasks(logical_time_to_be_exec_new):
    for logical_time in logical_time_to_be_exec_new:
        json = {LOGICAL_DATE: str(logical_time)}
        data = requests.post(url=URL, auth=(USER, PASS), json=json)
        print(data.json())


def main():
    logical_time_list = get_failed_logical_dates()
    logical_time_list.sort(reverse=True)
    logical_time_to_be_exec = pick_latest_logical_for_a_day(logical_time_list)
    logical_time_to_be_exec_new = add_one_sec_logical_time(logical_time_to_be_exec)

    print("Logical_time before adding 1 sec:")
    for logical_time in logical_time_to_be_exec:
        print(logical_time)

    print()
    print("Logical_time after adding 1 sec:")
    for logical_time in logical_time_to_be_exec_new:
        print(logical_time)

    trigger_failed_tasks(logical_time_to_be_exec_new)


if __name__ == "__main__":
    main()
