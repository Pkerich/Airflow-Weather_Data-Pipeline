from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'pker',
}


with DAG(
    dag_id = 'sql_pipeline',
    description = 'A simple SQL pipeline DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['sql', 'sqlite', 'database']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r'''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER NOT NULL,
            city VARCHAR(50),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag
    )

    insert_values = SqliteOperator(
        task_id = 'insert_values',
        sql = r'''
        INSERT INTO users (name, age, is_active) VALUES
            ('Alice', 30, false),
            ('Bob', 25, true),
            ('Charlie', 35, false),
            ('David', 28, false),
            ('Eve', 22, true);
            ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag
    )
    insert_values2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r'''        INSERT INTO users (name, age) VALUES
            ('Frank', 40),
            ('Grace', 27),
            ('Heidi', 32),
            ('Ivan', 29),
            ('Judy', 24);
            ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag
    )

    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r'''
        DELETE FROM users WHERE is_active = 0;
        ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag
    )

    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r'''
        UPDATE users SET city = 'New York';
        ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    display_results = SqliteOperator(
        task_id = 'display_results',
        sql = r'''
        SELECT * FROM users;
        ''',
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True 
    )

create_table >> [insert_values, insert_values2] >> delete_values >> update_values >> display_results
   