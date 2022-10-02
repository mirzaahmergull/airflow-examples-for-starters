from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.bash import BashOperator

def download_tasks():

    with TaskGroup("download_files", tooltip="Sub DAG which downloads the files") as group:

        download_a = BashOperator(
            task_id="download_a",
            bash_command='sleep 30',
        )

        download_b = BashOperator(
            task_id="download_b",
            bash_command='sleep 30',
        )

        download_c = BashOperator(
            task_id="download_c",
            bash_command='sleep 30',
        )

        return group