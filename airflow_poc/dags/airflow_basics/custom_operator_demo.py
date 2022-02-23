from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from operators.my_bash_operator import MyBashOperator




# class MyBashOperator(BashOperator):
#     # pass
#
#     # super().__init__()
#     def __init__(self, *, bash_command: str, **kwargs):
#         # print(kwargs)
#
#         for key in list(kwargs['params'].keys()):
#             if (kwargs['params'][key] == "" or kwargs['params'][key] is None):
#                 # print(kwargs['params'][key])
#                 print(kwargs['params'][key])
#                 del kwargs['params'][key]
#         # print(f"Params: {kwargs['params']['Name']}")
#         # for key, item in kwargs.items():
#         #     print(key, "-----", item)
#         super().__init__(bash_command=bash_command, **kwargs)
#         # print(kwargs)
#         # print("In BashOperator")


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='custom_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    params={"Name": "Airflow1", "key1": "value1", "key2": "value2", "key3": "value3"},
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    task1 = MyBashOperator(task_id='first_task',
                           bash_command="echo 'using custom operator'; echo 'Name: {{params.Name}} key1:{{"
                                        "params.key1}} key2: {{params.key2}} key3: {{params.key3}}'",
                           dag=dag,
                           params={"Name": "", "key1": None, "key2": "task_value2", "key3": "task_value3", "blank_filter_keys":["key1"]},)
