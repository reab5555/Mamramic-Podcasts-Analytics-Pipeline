from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import functions from source files
from src_scripts.raw_to_s3 import main as raw_to_s3_task
from src_scripts.download_podcasts import main as download_podcasts_task
from src_scripts.clean_XML import main as clean_xml_task
from src_scripts.logs_data_organize import main as logs_data_organize_task
from src_scripts.create_DWH import main as create_dwh_task
from src_scripts.quality_verify import quality_verify_task
from src_scripts.analytics import analyze_podcasts as analytics_task

default_args = {
   'owner': 'user',
   'depends_on_past': False, 
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=1),
}

with DAG(
   'run_operation-spotify',
   default_args=default_args,
   schedule_interval='0 1 1 * *',  # Run at 1:00 AM on the 1st day of each month
   start_date=datetime(2025, 1, 1),
   catchup=False,
) as dag:

   ingest_raw_to_s3 = PythonOperator(
       task_id='ingest_raw_to_s3',
       python_callable=raw_to_s3_task
   )

   ingest_download_podcasts = PythonOperator(
       task_id='ingest_download_podcasts', 
       python_callable=download_podcasts_task
   )

   staging_clean_xml = PythonOperator(
       task_id='staging_clean_xml',
       python_callable=clean_xml_task
   )

   staging_logs_data_organize = PythonOperator(
       task_id='logs_data_organize',
       python_callable=logs_data_organize_task
   )

   load_create_dwh = PythonOperator(
       task_id='create_dwh',
       python_callable=create_dwh_task
   )

   data_quality_verify = PythonOperator(
       task_id='quality_verify',
       python_callable=quality_verify_task
   )

   analytics = PythonOperator(
       task_id='analytics',
       python_callable=analytics_task
   )

   # Define task dependencies
   ingest_raw_to_s3 >> ingest_download_podcasts >> staging_clean_xml >> staging_logs_data_organize >> load_create_dwh >> data_quality_verify >> analytics