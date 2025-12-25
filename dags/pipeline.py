import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'pker',
}

def read_csv_file():
    df = pd.read_csv('/home/pker/airflow/dataset/insurance.csv')
    print("CSV file read successfully!")
    print(df.head())
    return df.to_json()
def remove_null_values(**kwargs):
    ti = kwargs['ti']
    json_data  = ti.xcom_pull(task_ids = 'read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)

    return df.to_json()
def groupby_smoker(ti):
    json_data  = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    smoker_df.to_csv('/home/pker/airflow/output/smoker_summary.csv', index=False)


def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    region_df.to_csv('/home/pker/airflow/output/region_summary.csv', index=False)


with DAG(
    dag_id = 'pipeline_read_csv',
    description = 'Read CSV file using Pandas',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pandas', 'csv', 'dataframe']
) as dag:
    read_csv_task = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
        
    )

    groupby_smoker = PythonOperator(
        task_id = 'groupby_smoker',
        python_callable = groupby_smoker
    )

    group_by_region = PythonOperator(
        task_id = 'groupby_region',
        python_callable = groupby_region
    )


    read_csv_task >> remove_null_values >> [groupby_smoker, group_by_region]
