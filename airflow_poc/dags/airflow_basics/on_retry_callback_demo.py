from datetime import datetime
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import requests
import html
import re


TEXT_TYPE_AND_COLOR = [
                       {
                          "text": "error",
                          "color": "red"
                       },
                       {
                          "text": "warning",
                          "color": "yellow"
                       },
                       {
                          "text": "exception",
                          "color": "brown"
                       },
                       {
                          "text": "info",
                          "color": "green"
                       }
                    ]


def change_text_color(content):
    for x in TEXT_TYPE_AND_COLOR:
        finds = re.compile(x['text'], flags=re.I)
        for find in finds.findall(content):
            content = content.replace(find, f"""<span style="color:{x['color']}">{find}</span>""")
    return content


def notify_email(contextDict, **kwargs):
    dag_id = contextDict['dag_run'].dag_id
    dag_run_id = contextDict['dag_run'].run_id
    task_id = contextDict['task_instance'].task_id
    task_try_number = contextDict['task_instance']._try_number

    title = f"Airflow Alert: {task_id} try no: {task_try_number} of {dag_id} Failed."
    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
    username = "admin"
    password = "admin"
    params = {"full_content": "true"}
    response = requests.get(url=url, auth=(username, password), params=params)


    # response_content = response.content.decode('ISO-8859-1')
    # response_content = html.escape(response_content)
    # response_content = change_text_color(response_content)
    response_content = change_text_color(html.escape(response.content.decode('ISO-8859-1')))
    response_content = """<pre> <div style="font-family: Arial; font-size: 15px;">""" + response_content + "</pre> </div>"

    body = f"""
            Dag Id: {dag_id} <br>
            Dag Run Id: {dag_run_id} <br>
            Task Id: {task_id} <br>
            Try Number: {task_try_number} <br> 
            {response_content}
    """
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

dag = DAG(dag_id='on_retry_callback_demo', default_args=args, schedule_interval='0 5 * * *', render_template_as_native_obj=False, )


with dag:
    py_task = KubernetesPodOperator(
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
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--elt_by',
            '{{ ds }}'
        ],
    )

py_task
