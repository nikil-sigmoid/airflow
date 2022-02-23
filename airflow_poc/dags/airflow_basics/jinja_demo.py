from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from macros.greet_hello import say_hello
from filters.make_upper import make_upper


args = {
    'owner': 'airflow',
}


# def say_hello(name):
#     # print("saying hello from my side!")
#     return f"Hi, {name}! Saying hello from my side!"


# date = '{{ ds }}'
# print(f"date: {date}")


# def make_upper(name):
#     # return "Hello: " + name
#     return name.upper()


# default_args = {
#         'start_date': datetime(2018, 5, 19),
#         'user_defined_filters': dict(hello=lambda name: 'Hello%s' % name, filter2make_upper),
#         }


# sfmc_elt_by = "{{ dag.dag_id }}___{{task_id}}___{{ run_id }}"


temp_var = 'Fooo87'

with DAG(
    dag_id='jinja_demo',
    default_args=args,
    schedule_interval='0 0 * * *',
    params={"Name": "Airflow1"},
    start_date=days_ago(2),
    user_defined_macros={'say_hello': say_hello},
    # user_defined_filters=dict(hello=lambda name: f"Hello: {name.capitalize()}", my_filter=make_upper),
    # user_defined_filters=dict(hello=lambda name: f"Hello: {name.capitalize()}"),
    user_defined_filters={'my_filter': make_upper},
    jinja_environment_kwargs={
        'variable_start_string': '<<',
        'variable_end_string': '>>',
    },
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    # task1 = BashOperator(task_id='first_task', bash_command="""echo 'greeting....: {{ greet_hello(name="Foo") }}'""", dag=dag)
    task1 = BashOperator(task_id='first_task', bash_command=f"""echo "greeting: << say_hello(name='{temp_var}') >>" """, dag=dag)
    task2 = BashOperator(task_id='second_task', bash_command="echo 'filtering....: << \"boo\" | my_filter >>'", dag=dag)

    task1 >> task2


# jinja_env = dag.get_template_env()
# # print(jinja_env.filters)
#
#
# for filter in jinja_env.filters:
#     print(filter)