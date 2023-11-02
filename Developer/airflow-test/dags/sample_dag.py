from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from utils.utils import local_to_s3

BUCKET_NAME = Variable.get("BUCKET")
AWS_ID = Variable.get("AWS_ID")
IAM_ROLE_NAME = Variable.get("IAM_ROLE_NAME")


script_extract_data_path = './scripts/extract_data.sql'
script_create_external_database_path = './scripts/create_external_database.sql'
script_create_external_table_path = './scripts/create_external_table.sql'
script_drop_table_path = './scripts/drop_table.sql'

employee_raw_data_path = 'temp/employee_salaries.csv'

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "user_behaviour",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 11, 1),
    max_active_runs=1
)

empty_task = EmptyOperator(task_id='empty_task')


extract_employee_salaries_data = SQLExecuteQueryOperator(
    dag=dag,
    task_id='extract_employee_salaries_data',

    sql=script_extract_data_path,
    params={'employee_salaries': "/temp/employee_salaries.csv"},
    conn_id='postgres_default',
    depends_on_past=True,
    wait_for_downstream=True
)

employee_salaries_data_to_data_lake = PythonOperator(
    dag=dag,
    task_id='employee_salaries_data_to_data_lake',
    python_callable=local_to_s3,
    op_kwargs={
        "file_name": employee_raw_data_path,
        "key": "stage/employee_salaries.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": True,
    },
)
move_data_from_data_lake_to_data_warehouse = SQLExecuteQueryOperator(
    dag=dag,
    task_id='move_data_from_data_lake_to_data_warehouse',
    sql=[
         script_create_external_database_path,
         script_drop_table_path,
         script_create_external_table_path,
         ],
    params={
        'AWS_ID': AWS_ID,
        'IAM_ROLE_NAME':IAM_ROLE_NAME,
        'bucket_name': BUCKET_NAME
        },
    autocommit=True,
    conn_id='redshift-conn-id',
    depends_on_past=True,
    wait_for_downstream=True
)

extract_employee_salaries_data >> employee_salaries_data_to_data_lake >> move_data_from_data_lake_to_data_warehouse >> empty_task
