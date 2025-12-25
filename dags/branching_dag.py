from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from random import choice

from airflow.operators.python import BranchPythonOperator   


default_args = {
    'owner': 'pker'
}
def has_driving_license():
    return choice([True, False])
def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'can_drive_to_work'
    else:
        return 'not_eligible_to_drive'
def drive_to_work():
        print("Driving to work!")
def not_eligible_to_drive():
        print("Kindly enrol in a driving lesson and get a license.")

with DAG(
    dag_id = 'branching_dag',
    description = 'A DAG with branching logic',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'example']
) as dag:


    check_license = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_driving_license
    )

    branching = BranchPythonOperator(
        task_id = 'branching',
        python_callable = branch
    )

    drive_task = PythonOperator(
        task_id = 'can_drive_to_work',
        python_callable = drive_to_work
    )

    cannot_drive_task = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive
    )

    check_license >> branching >> [drive_task, cannot_drive_task]