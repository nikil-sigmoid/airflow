from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}


def say_hello(name, greet, city, country):
    # print("saying hello from my side!")
    return f"Hi, {name}! Saying hello from my side! {greet} {city} {country} "


def user_filter(name):
    # return "Hello: " + name
    return name.upper()


temp_var1 = 'Fooo87'
temp_var2 = 'Booo87'

var1 = "dfdsdsf"

dag = DAG('jinja_multi_args_k8s',
          schedule_interval='@once',
          default_args=default_args,
          user_defined_filters={'my_filter': user_filter},
          jinja_environment_kwargs={
              'variable_start_string': '<<',
              'variable_end_string': '>>',
          },
          user_defined_macros={'greet_hello': say_hello},
          params={'sfmc_elt_by': '<< ds >>', 'var11': var1},
          # render_template_as_native_obj=True
          )

with dag:
    task1 = KubernetesPodOperator(
        resources={},
        namespace='default',
        image="nikilr/hello_test1:dev",
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=True,
        cmds=["python", "-c", "print('hello task 1 ..................')"],
        # params={'sfmc_elt_by': "{{ds}}"},
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--output_bucket_name',
            'working-bucket-name',
            '--input_path_filenames',
            'output_file_json',
            '--output_path',
            'appended_csv',
            '--output_filename',
            'csv combined filename',
            '--elt_by',
            '<< ds >>',
            # '{{ params.sfmc_elt_by }}'
            f"""<<
            greet_hello(name='{temp_var1}',
                               city = '{temp_var2}',
                               country = 'USA',
                               greet = 'Good morning!') 
            >>""",
            '<< params.sfmc_elt_by >>',
            '<< params.var11 >>'
        ],
    )
