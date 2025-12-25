import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'pker',
}

def increment_by_1(value):
    value_int = value
    print("Value {value}!".format(value=value_int))
    return value_int + 1

def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='increment_by_1')
    print("Value {value}!".format(value=value))
    return value * 100
def substract_9(ti):
    value = ti.xcom_pull(task_ids='multiply_by_100')
    print("Value {value}!".format(value=value))
    return value - 9
def print_value(ti):
    value = ti.xcom_pull(task_ids='substract_9')
    print("Final Value {value}!".format(value=value))

with DAG(
    
    dag_id = 'cross_task_communication',
    description = "Cross-task communication with XCom",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python']
) as dag:
    task1 = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs = {'value': 1},
    )
    
    task2 = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100,
    )   

    task3 = PythonOperator(
        task_id = 'substract_9',
        python_callable = substract_9,
    )   
    task4 = PythonOperator(
        task_id = 'print_value',
        python_callable = print_value,
    )   
    task1 >> task2 >> task3 >> task4 