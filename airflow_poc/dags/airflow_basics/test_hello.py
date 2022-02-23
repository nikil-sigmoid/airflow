from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.context import VariableAccessor
from airflow.utils.dates import days_ago
from airflow.models import Variable


def get_var_3():
    try:
        return Variable.get('var_3')
    except:
        return None

var_4 = "great"

var_1 = get_var_3()
var_2 = Variable.get("var_2")
var_3 = var_1 if var_1 else var_2

# VariableAccessor()

print("Starts from here")

default_arguments = {"owner": "{{ds}}", "start_date": days_ago(11)}

def get_parameters(**kwargs):
    # dag_run = kwargs.get('dag_run')
    # parameters = dag_run.conf['key']
    return kwargs

# var_1


def hello_world(*args, **kwargs):
    # print("Hello world! This is sample code for python operator.")
    # for i in args:
    #     print(i)

    global var_4
    print(f"should print great: {var_4}")
    var_4 = "awesome"
    print(f"should print awesome: {var_4}")

    for key, value in kwargs.items():
        print(f"Key is {key}: {value}")

    print(kwargs["Project"])


with DAG(
    "test_hello",
    # schedule_interval="@hourly",
    catchup=False,
    default_args=default_arguments,
    # max_active_runs=1,
) as dag:

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        op_args=["Hello", "I'm good.", "How are you?"],
        op_kwargs={"Name": "python", "Project": "Airflow", "version": "2.0"},
    )

    say_bye = BashOperator(task_id="say_goodbye",
                           # bash_command="""echo param value: {{params.param_1}}""",
                           bash_command=f"""echo should print awesome: {var_4}""",
                           # params={"param_1": get_parameters()}
                           )


say_hello >> say_bye


