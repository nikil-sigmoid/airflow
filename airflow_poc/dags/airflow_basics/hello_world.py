from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

print("Starts from here")

default_arguments = {"owner": "Nikhil",
                     "start_date": days_ago(1),
                     'email': ['nikilr@sigmoidanalytics.com'],
                     'email_on_failure': True}


def hello_world():
    print("Hello world! This is sample code for python operator.")


with DAG(
        "hello-world",
        schedule_interval="@hourly",
        catchup=False,
        default_args=default_arguments,
        max_active_runs=1,
) as dag:
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
    )

    say_goodbye = BashOperator(
        task_id="say_goodbye",
        bash_command="echo closing in 10 seconds; sleep 5; thyth",
    )


    say_hello >> say_goodbye
