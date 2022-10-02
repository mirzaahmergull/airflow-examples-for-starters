from platform import python_branch
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import json
from pandas import json_normalize


def _store_user(ti):
    store_user = PostgresHook(postgres_conn_id="postgres")
    store_user.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename="/tmp/processed_user.csv",
    )


def _process_users(ti):
    users = ti.xcom_pull(task_ids="extract_users")
    print("USERS IS ", users)
    user = users["results"][0]
    print("user is " , user)
    processed_user = json_normalize(
        {
            "firstname": user["name"]["first"],
            "lastname": user["name"]["last"],
            "country": user["location"]["country"],
            "email": user["email"],
            "username": user["login"]["username"],
            "password": user["login"]["password"],
        }
    )
    print(processed_user)
    processed_user.to_csv("/tmp/processed_user.csv", index= False, header=False)
    return


with DAG(
    "user_processing",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_table = PostgresOperator(
        task_id="create_table_users",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users(
                firsname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        """,
    )

    is_api_avail = HttpSensor(
        task_id="api_available", http_conn_id="user_api", endpoint="api/"
    )

    extract_users = SimpleHttpOperator(
        task_id="extract_users",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    process_users = PythonOperator(
        task_id="process_users",
        python_callable=_process_users,
    )

    store_user= PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )

    create_table >> is_api_avail >> extract_users >> process_users >> store_user
