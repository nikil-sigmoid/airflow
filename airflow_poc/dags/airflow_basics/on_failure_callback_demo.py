from datetime import datetime
from pprint import pprint
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import requests
import logging
import html
import re



some_text = """
<pre>
<div style="font-family: Arial; font-size: 15px;">
[2022-02-10 13:55:52,252] {kubernetes_pod.py:459} INFO - Deleting pod: airflow-test-pod1.8397e4599438463381fd31e734feb0a8
[2022-02-10 13:55:52,385] {taskinstance.py:1700} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/Users/nik/opt/anaconda3/envs/airflow_poc/lib/python3.9/site-packages/airflow/models/taskinstance.py", line 1329, in _run_raw_task
    self._execute_task_with_callbacks(context)
  File "/Users/nik/opt/anaconda3/envs/airflow_poc/lib/python3.9/site-packages/airflow/models/taskinstance.py", line 1455, in _execute_task_with_callbacks
    result = self._execute_task(context, self.task)
  File "/Users/nik/opt/anaconda3/envs/airflow_poc/lib/python3.9/site-packages/airflow/models/taskinstance.py", line 1511, in _execute_task
    result = execute_callable(context=context)
  File "/Users/nik/opt/anaconda3/envs/airflow_poc/lib/python3.9/site-packages/airflow/providers/cncf/kubernetes/operators/kubernetes_pod.py", line 430, in execute
    self.cleanup(
  File "/Users/nik/opt/anaconda3/envs/airflow_poc/lib/python3.9/site-packages/airflow/providers/cncf/kubernetes/operators/kubernetes_pod.py", line 452, in cleanup
    raise AirflowException(f'Pod {pod and pod.metadata.name} returned a failure: {remote_pod}')
airflow.exceptions.AirflowException: Pod airflow-test-pod1.8397e4599438463381fd31e734feb0a8 returned a failure: {'api_version': 'v1',
 'kind': 'Pod',
 'metadata': {'annotations': {'some-key': 'some-value'},
              'cluster_name': None,
              'creation_timestamp': datetime.datetime(2022, 2, 10, 8, 25, 45, tzinfo=tzutc()),
              'deletion_grace_period_seconds': None,
              'deletion_timestamp': None,
              'finalizers': None,
              'generate_name': None,
              'generation': None,
 <span style="color: red"> Error 9787 </span>
 </div>
</pre>
"""


# Create and configure logger
logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def str_to_bytes():
    str = "Welcome to Geeksforgeeks"

    arr = bytes(str, 'utf-8')

    return arr


def python_to_html(response_content):
    return response_content.replace(b"\n", b"poiuyt")

TEXT_TYPE_AND_COLOR = [
                       {
                          "text":"error",
                          "color":"red"
                       },
                       {
                          "text":"warning",
                          "color":"yellow"
                       },
                       {
                          "text":"exception",
                          "color":"brown"
                       },
                       {
                          "text":"info",
                          "color":"green"
                       }
                    ]


def change_text_color(response_content):
    response_content = re.sub(r'error', 'grassroot', response_content, flags=re.I)



def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    # title = "Airflow alert: demo_task1 Failed".format(**contextDict)

    dag_id = contextDict['dag_run'].dag_id
    dag_run_id = contextDict['dag_run'].run_id
    task_id = contextDict['task_instance'].task_id
    task_try_number = contextDict['task_instance']._try_number
    token = ""

    # title = f"Airflow alert: demo_task1 Failed: context_dict_details:{contextDict} kwargs_details:{kwargs}"
    title = f"Airflow Alert: {task_id}(dag: {dag_id}) Failed."

    # print(contextDict)

    # email contents

    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"

    username = "admin"
    password = "admin"
    # PARAMS = {"dag_id":"on_failure_callback", "dag_run_id":"manual__2022-02-09T12:54:55.216951+00:00", "task_id":"task-one", "task_try_number":"1","full_content":"true"}
    params = {"full_content": "true"}

    bytes_1 = str_to_bytes()
    str_1 = bytes_1.decode('utf-8')

    response = requests.get(url=url, auth=(username, password), params=params)

    logger.info("Will be storing response:")
    logger.info(response)

    response_content = python_to_html(response.content)

    response_content = response_content.decode('ISO-8859-1')
    response_content1 = type(response.content)
    response_content2 = type(str(response.content))
    name = "ABC"
    response_text = response.text
    name_type = type(name)

    # response_content = python_to_html(response.content)


    # Task: {contextDict['task'].__dict__} < br >
    # Task_Instance: {contextDict['task_instance'].__dict__} < br >
    # Task_instance_key_str: {contextDict['task_instance_key_str']} < br >
    # Task_log: {contextDict['task_instance']._log.__dict__} < br >
    # Task_Instance_log: {contextDict['task']._log.__dict__} < br >

    # body = f""" context_dict_details:{contextDict} kwargs_details:{kwargs} <br>
    # Hi Everyone, <br>
    # <br>
    # There's been an error in the demo_task1 job.<br>
    # <br>


    sample_html = """
    Br is given below
    <br> 
    sample html file.<br>
    <p> this is para </p>
    para ended already
    """

    final_html = html.escape(response_content).replace("poiuyt", "<br>").replace("exception", "<span style='color:red'> exception </span>")



    body = f"""
            {some_text}
            Dag Id: {dag_id} <br>
            Dag Run Id: {dag_run_id} <br>
            Task Id: {task_id} <br>
            Try Number: {task_try_number} <br> 
            Response454:{response.content}
    """
            # Response24352:{final_html}
            # Response2343:{html.escape(sample_html)}
            # Response243:{response.content} <br>
            # Content_decode-str:{isinstance(response_content, str)} <br>
            # response_text3534:{response_text} <br>
            # response_content4564: {response.content} <br>
            # {response_content1} <br>
            # {response_content2} <br>
            # name: {name} <br>
            # name_type: {name_type} <br>
            # name-str:{isinstance(name, str)} <br>
            # content1-bytes:{isinstance(response_content1, bytes)} <br>
            # content2-str:{isinstance(response_content2, str)} <br>
            # bytes_1-bytes:{isinstance(bytes_1, bytes)} <br>
            # bytes_1: {bytes_1} <br>
            # str_1-str-str:{isinstance(str_1, str)} <br>
            # str_1: {str_1} <br>
    # Forever yours,<br>
    # Airflow bot <br>
    # # *Dag567*: {contextDict['dag_run'].dag_id} <br>
    # *Dag789*: {contextDict['dag_run'].__dict__} <br>
    # *Task213*: <{contextDict.get('task_instance').log_url}|*{contextDict.get('task_instance').task_id}* failed for execution {contextDict.get('execution_date')}>,
    # *Task789*: {contextDict['task_instance'].__dict__} <br>
    # *Task789*: {contextDict['task_instance'].task_id} <br>
    # *Task789*: {contextDict['task_instance']} <br>
    # """

    send_email('nikilr@sigmoidanalytics.com', title, body)




def on_failure_callback(context):
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print(task_instances)
    """Send custom email alerts."""

    # email title.
    # title = "Airflow alert: demo_task1 Failed".format(**contextDict)
    title = f"Airflow alert: demo_task1 Failed"

    print(context)

    # email contents
    body = f""" context_dict_details:{task_instances} """

    send_email('nikilr@sigmoidanalytics.com', title, body)


args = {
  'owner': 'me',
  'description': 'my_example',
  'start_date': days_ago(1),
  'on_retry_callback':notify_email,
  'on_failure_callback':notify_email,
  'retries' : 3,
  'retry_delay': timedelta(seconds=3)
}

# run every day at 12:05 UTC
dag = DAG(dag_id='on_failure_callback_demo', default_args=args, schedule_interval='0 5 * * *', render_template_as_native_obj=False, )

# def print_hello():
#   return 'hello!'


# py_task = PythonOperator(task_id='example',
#                          python_callable=print_hello,
#                          on_failure_callback=notify_email,
#                          dag=dag)

# py_task = BashOperator(
#     task_id="demo_task1",
#     bash_command="echo 'closing...'; thyth",
#     dag=dag,
#     on_failure_callback=notify_email,
#     # on_failure_callback=on_failure_callback,
#     email_on_failure=True,
#     params={"key1": "{{ds}}"},
#     arguments=["args1", "args2"]
#
#
# )




with dag:
    py_task = KubernetesPodOperator(
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
        cmds=["python", "-c", "printfgsd('hello task 1 ..................')"],
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
            '{{ ds }}'
            # '{{ params.sfmc_elt_by }}'
        ],

        # on_failure_callback=notify_email

        # service_account_name='build-robot',
    )

py_task


# \n ojhfdodo
# escape
# ojhnodf <br>

"""
Scenario-1:
If we don't escape: Objects not printed, but <br> works

Scenario-2:
If we use escape: Objects are printed but <br> doesn't work

Scenario-3:



"""