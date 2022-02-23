from pprint import pprint

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    # title = "Airflow alert: demo_task1 Failed".format(**contextDict)
    title = f"Airflow alert: demo_task1 Failed: context_dict_details:{contextDict} kwargs_details:{kwargs}"

    print(contextDict)

    # email contents
    body = f""" context_dict_details:{contextDict} kwargs_details:{kwargs} <br>
    Hi Everyone, <br>
    <br>
    There's been an error in the demo_task1 job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    # *Dag567*: {contextDict['dag_run'].dag_id} <br>
    *Dag789*: {contextDict['dag_run'].__dict__} <br>
    *Task213*: <{contextDict.get('task_instance').log_url}|*{contextDict.get('task_instance').task_id}* failed for execution {contextDict.get('execution_date')}>,
    *Task789*: {contextDict['task_instance'].__dict__} <br>
    *Task789*: {contextDict['task_instance'].task_id} <br>
    *Task789*: {contextDict['task_instance']} <br>
    """

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
  'on_failure_callback':notify_email
}

# run every day at 12:05 UTC
dag = DAG(dag_id='on_failure_callback_test', default_args=args, schedule_interval='0 5 * * *', render_template_as_native_obj=True)

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