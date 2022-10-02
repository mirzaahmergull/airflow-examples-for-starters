from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
def _t1(ti):
    ti.xcom_push(key="my_key", value=101)
    return
 
def _t2(ti):
    ti.xcom_push(key="my_key", value=102)

def _t3(ti):
    print("T3 RAN")

def _t4(ti):
    print("T4 RAN")

def _t5(ti):
    print("T5 RAN")

def _first_branch(ti):
    value_t1= ti.xcom_pull(key="my_key", task_ids = 't1')
    value_t2= ti.xcom_pull(key="my_key", task_ids = 't2')
    if value_t1 == value_t2:
        return 't3'
    else:
        return 't4'
    None
 
with DAG("branch_dag", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    branch = BranchPythonOperator(
    task_id="branch_t1_t2_task",
    python_callable=_first_branch,
    )
 
    t3 = PythonOperator(
        task_id='t3',
        python_callable=_t3
    )

    t4 = PythonOperator(
        task_id='t4',
        python_callable=_t4
    )

    t5 = PythonOperator(
        task_id='t5',
        python_callable=_t5,
        trigger_rule='none_failed'

    )
 
    [t1,t2] >> branch >> [t3,t4] >> t5