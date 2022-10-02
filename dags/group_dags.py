from airflow import DAG
from airflow.operators.bash import BashOperator
from group_tasks import group_downloads,group_transform
 
from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    download_files= group_downloads.download_tasks()
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_files = group_transform.transform_tasks()
 
    download_files >> check_files >> transform_files