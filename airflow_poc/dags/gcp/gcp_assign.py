from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.utils.dates import days_ago
#
PROJECT_ID = Variable.get("project")
LANDING_BUCKET = Variable.get("landing_bucket")
# BACKUP_BUCKET = Variable.get("backup_bucket")

default_arguments = {"owner": "Nikhil", "start_date": days_ago(1)}


def list_objects(bucket=None):
    hook = GoogleCloudStorageHook()
    storage_objects = hook.list(bucket)

    return storage_objects
#
with DAG(
    "gcs_to_bq_assign_test",
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_arguments,
    max_active_runs=1,
    user_defined_macros={"project": PROJECT_ID},
) as dag:

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_objects,
        op_kwargs={"bucket": LANDING_BUCKET},
    )
#
    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id="load_data",
        bucket=LANDING_BUCKET,
        source_objects=["*"],
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        destination_project_dataset_table="{{ project }}.gcp_airflow_assign.csvdata",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        bigquery_conn_id="google_cloud_default",
        google_cloud_storage_conn_id="google_cloud_default",
    )
#
    query = """
        SELECT * FROM `{{ project }}.gcp_airflow_assign.csvdata` as csvdata;
        """
#
    create_table = BigQueryOperator(
        task_id="create_table",
        sql=query,
        destination_dataset_table="{{ project }}.gcp_airflow_assign.csvdata",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        use_legacy_sql=False,
        location="us-east1",
        bigquery_conn_id="google_cloud_default",
    )
#
#
list_files >> load_data >> create_table