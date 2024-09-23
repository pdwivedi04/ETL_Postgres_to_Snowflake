from airflow import DAG # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_data
from config import SNOWFLAKE_CONN
#from sqlalchemy import create_engine # type: ignore
#from config import POSTGRES_CONN
 
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
 
dag = DAG(
    'archive_activity_assignment',
    default_args=default_args,
    description='Archive Activity Assignment Data',
    schedule_interval='@weekly',
)
 
# def delete_data():
#     engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}")
#     with engine.connect() as conn:
#         conn.execute("DELETE FROM master_config.Activity_assignment")
 
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)
 
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    #op_kwargs={'activity_data': "{{ task_instance.xcom_pull(task_ids='extract_data') }}"},
    dag=dag,
)
 
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    #op_kwargs={'activity_data': "{{ task_instance.xcom_pull(task_ids='transform_data') }}"},
    dag=dag,
)
 
# delete_task = PythonOperator(
#     task_id='delete_data',
#     python_callable=delete_data,
#     dag=dag,
# )
 
extract_task >> transform_task >> load_task
 