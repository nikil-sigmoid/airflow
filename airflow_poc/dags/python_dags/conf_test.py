# Trigger dag with config wala explore karna hai
# Uske variables me jo value pass ki hai use read karke dag me print karna hai
# Intention ye hai ki waha se hum airflow variables ki value ko bypass kar de
#


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG("conf_test", schedule_interval=None, start_date=days_ago(2))


def hello_python(**kwargs):
    pass
    # my_name = kwargs['dag_run'].conf.get('my_name')
    my_name = kwargs['my_name']
    print("Start here:")
    print(my_name)
    # print({{params.my_param}})
    print("Ends here.")

parameterized_task = PythonOperator(
    task_id="parameterized_task",
    python_callable=hello_python,
    dag=dag,
    provide_context=True,
    op_kwargs= {"my_name" : "Batmanmhbvg1"},
    params={'my_param': 'Parameter I passed in'}
    # params={"my_name" : "Batman1"}
)

# airflow dags trigger --conf '{"conf1": "value1"}' example_parameterized_dag
# airflow dags trigger --conf '{"my_name": "Batman"}' conf_test


# Airflow UI: Configuration JSON (Optional, must be a dict object)
# {"my_name" : "Batman1"}

# Airflow CLI:
# airflow dags trigger --conf '{"my_name": "Superman1"}' conf_test




#
# def SendEmail(**kwargs):
#     print(kwargs['key1'])
#     print(kwargs['key2'])
#     msg = MIMEText("The pipeline for client1 is completed, please check.")
#     msg['Subject'] = "xxxx"
#     msg['From'] = "xxxx"
#     ......
#     s = smtplib.SMTP('localhost')
#     s.send_message(msg)
#     s.quit()
#
#
# t5_send_notification = PythonOperator(
#     task_id='t5_send_notification',
#     provide_context=True,
#     python_callable=SendEmail,
#     op_kwargs={'key1': 'value1', 'key2': 'value2'},
#     dag=dag,
# )