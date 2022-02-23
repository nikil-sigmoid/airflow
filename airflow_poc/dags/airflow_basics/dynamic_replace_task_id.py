from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

args = {
    'owner': 'airflow',
}


def say_hello(name):
    # print("saying hello from my side!")
    return f"Hi, {name}! Saying hello from my side!"


# date = '{{ ds }}'
# print(f"date: {date}")


def userfilter(name):
    # return "Hello: " + name
    return name.upper()


# "sfmc_elt_by": "{{ dag.dag_id }}___{{task_id}}___{{ run_id }}",
sfmc_elt_by = """echo 'dag id: {{dag.dag_id}} task id:{{task.task_id}} run id:{{ run_id }} {{params.temp_var}} '"""

var1 = '{{params.temp_var}}'




with DAG(
        dag_id='dynamic_replace_task_id',
        default_args=args,
        schedule_interval='0 0 * * *',
        # params={"Name": "Airflow1"},
        start_date=days_ago(2),
        user_defined_macros={'greet_hello': say_hello},
        user_defined_filters={'my_filter': userfilter},
        params={"temp_var": "value_dag"},
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    task1 = BashOperator(task_id='first_task',
                         name='{{task.task_id}}',
                         bash_command=sfmc_elt_by, dag=dag, params={'temp_var': None})

    # task1

jinja_env = dag.get_template_env()
# print(jinja_env.filters)


for filter in jinja_env.filters:
    print(filter)
