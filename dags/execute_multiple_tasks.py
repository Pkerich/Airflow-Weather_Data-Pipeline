from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'pker',
}   
with DAG(
    dag_id='execute_multiple_tasks',
    description='Execute multiple tasks in parallel',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['upstream', 'downstream']
) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command = '''
        echo "Executing Task 1"
        
        for i in {1..10}
        do 
            echo Task 1 iteration $i
        done
        echo "Task 1 completed"
        '''
    )

    task2 = BashOperator(
        task_id='task_2',
        bash_command= '''
        echo "Executing Task 2" 
        sleep 4
        echo "Task 2 completed"
        '''

    )
    
    task3 = BashOperator(
        task_id = 'task_3',
        bash_command = '''
        echo "Executing Task 3"
        sleep 15
        echo "Task 3 completed"
        '''
    )

    task4 = BashOperator(
        task_id = 'task_4',
        bash_command = '''
        echo "Task 4 completed"
        '''
        )

    task1 >> [task2, task3]
    
    
    task4 << [task2, task3]
   
