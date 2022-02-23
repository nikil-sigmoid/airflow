import datetime

import requests

DAG_ID = "test_hello"
URL = f"http://localhost:8080/api/v1/dags/{DAG_ID}/dagRuns"

USER = "your_airflow_username"
PASS = "your_airflow_password"
LOGICAL_DATE = "logical_date"
NUM_DAYS = 5
STR_FRMT = "%Y-%m-%dT%H:%M:%S.%f+10:00"

base = datetime.datetime.today() + datetime.timedelta(days=-5)

EXECUTION_DATES = [(base - datetime.timedelta(days = x)).strftime(STR_FRMT) for x in range(NUM_DAYS)]


def trigger_dag(logical_time_to_be_exec_new):
	# for time in logical_time_to_be_exec_new:
	# 	print(time)
	for logical_time in reversed(logical_time_to_be_exec_new):
		json = {LOGICAL_DATE: str(logical_time)}
		data = requests.post(url = URL, auth = (USER, PASS), json = json)
		print(data.json())


def main():


# for logical_time in EXECUTION_DATES:
# 	print(logical_time)


	trigger_dag(EXECUTION_DATES)

if __name__ == "__main__":
	main()

data = requests.get(url=URL, auth=(USER, PASS))



# print()
# print()
# for d in data.json():
# 	print(d)

# print(data.json())
#

# print(data.json()['total_entries'])
# for i in range(data.json()['total_entries']):
# 	print(data.json()['dag_runs'][0]['logical_date'], data.json()['dag_runs'][0]['state'])
