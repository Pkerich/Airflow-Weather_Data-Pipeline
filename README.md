
# Building Weather Data Pipeline Using Airflow


A brief description of what this project does and who it's for


## API Reference

#### OpenWeatherMap website
https://home.openweathermap.org/api_keys

Create an account
Login in 
On the top right hand corder under the profile name, click the drop_down arrow and click on My API Keys to display API key

Note: It might take up to 24 hours for the API key to be activated 

#### OpenWeatherMap API Documentation Page

```http
  https://openweathermap.org/api/one-call-3
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | **Required**. Your API key |






## Introduction
Originally developed at Airbnb and now a global industry standard, Apache Airflow is an open-source platform used to programmatically author, schedule, and monitor complex workflows. It is widely celebrated for its Python-based "Configuration as Code" approach, highly extensible modular architecture, and intuitive web interface. 
While Airflow is rooted in traditional ETL (Extract, Transform, Load) paradigms, it models workflows as Directed Acyclic Graphs (DAGs). In a DAG, tasks are represented as nodes and their dependencies as directed edges, ensuring that the workflow follows a precise execution path without cycles or infinite loops. For example, a standard ETL pipeline would be defined as a linear sequence where the Load task depends on the Transform task, which in turn depends on the Extract task. 
By 2025, Airflow has evolved beyond simple batch processing to support Airflow 3.0 features such as event-driven scheduling, built-in DAG versioning, and a modernized React-based UI, making it a critical foundation for modern MLOps and AI pipelines.
Airflow architecture comprises of the following components:

•	Scheduler

•	Executor

•	Metadata database

•	Web-server UI

The scheduler is responsible for scheduling tasks and it relies on the set start date and the interval components which need to be specified in the DAG instantiation python code. The already scheduled DAG is executed by the executor. Notably, the executor does not run the logic within the task, rather, it allocates the task to the configured resources to run it. An instance of a task running is referred to as a DAG run. The information on the task run, their success and associated tasks together with the schedule is stored in the metadata database. The metadata database also stores information on user defined variables and other information such as connections. 
The webserver UI provides a visual interface to interact with airflow. 
There are several ways to handle the installation of Airflow. The UI list the DAGs, their status, logs, and other pertinent information required to rerun, troubleshoot, and monitor the data pipeline. 

## Installation
There are several ways to install Apache Airflow

•	Locally

•	Cloud platform (EC2 on AWS)

•	Docker (Astro CLI)

In this case I will demonstrate local installation

### System Requirements 
#### Windows WSL or MacOS
Since I am using Windows Machine, I activated WSL on Windows Features settings and restarted my PC. Then I installed Ubuntu via Microsoft Store. Once Ubuntu is running, create a user and assign a password. Thereafter run the following commands

    sudo apt update && sudo apt upgrade

I will be using Python 3.7 to run Airflow 2.7 and hence will be setting up a virtual environment for it. 

Install Python 3.7 and create a virtual environment and name it appropriately. Make sure you are the root directory

    cd ~
    pip install venv
    venv airflow_env
    source airflow_env/bin/activate

Run python –version to confirm Python 3.7 is installed

    python -version

Before installing airflow, we need to set up an airflow directory. Create a folder named airflow at anywhere you desire and cd to it. Then run pwd command to show your current location and copy the full path. 
    mkdir airflow
    pwd
Then edit the ~/.bashrc file using nano or vi and add the following line to set the present path as an environmental variable. It should look like this. 
    AIRFLOW_HOME=/home/pked/airflow
So any time you open a new terminal, you are able to recover the value of the environmental variable by running this command. 
    cd /
    pwd
    cd $AIRFLOW_HOME
Now install airflow using the command

    
    pip install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"

Once done, run the below commands to show the version of airflow running and other available commands

At this point, you will realize that the airflow directory contains several files created as a result of the successful installation. Once such file is the configuration file airflow.cfg. Inspect the file by running the commands as below.

    airflow.cfg help

Now initialize the database by running the command below and you will realize that the airflow.db is created

    airflow db init

Then create a user. First run airflow users -help to display all the available options and airflow users list to list all the users. No user has been created yet. Now run the command below to create a user. 

    airflow users -help
    airflow users list

To create a user run, with your username, names, email, and password
    
    airflow users create --username admin --firstname P --lastname Ker --role Admin --email k@gmail.com --password xxxx

To display users, run

    airflow users list

Now that everything is ready, run

    airflow scheduler &

Then open a new terminal and run 

    airflow webserver &

Go to http://localhost:8080/login and log in using username and password created. The landing page should bring up something like this. 

Once logged in, click on DAGs to show a list of default DAGs. 
Explore the various dags shown as examples and associated concepts like their schedule and their states. As seen, these are many dags and it gets a little confusing to analyze.


Our focus is to write our own dags and run them. Thus we will disable the example dags from loading when we fire up our scheduler by making some configurations in the airflow.cfg. 
Run the command vi airflow.cfg and edit load_examples to “False” as shown. Save and exit. 

    vi airflow.cfg

To write a dag, we first need to create a dags subfolder within the airflow home directory. Next, create weather_dag.py inside the dags folder. Populate the weather.py file with the below code and save. Refresh the UI and voila! The dag is there. 

    import sys
    import os
    from datetime import datetime, timedelta
    from airflow.utils.dates import days_ago
    from airflow import DAG
    import time
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.operators.postgres import PostgresOperator


    file_path = os.path.abspath(__file__) #gets the absolute path of the current file
    DAG_FOLDER = os.path.dirname(file_path) #gets the directory name of the file path
    SCRIPT_FOLDER = os.path.join(DAG_FOLDER, 'weather_etl')     #constructs the path to the 'weather_etl' folder
    sys.path.append(SCRIPT_FOLDER)  
    import main

    default_args = {
    'owner' : 'pker',

    }

    with DAG(
        dag_id = 'weather_dag',
        description = 'Extract weather data from api',
        default_args = default_args,
        start_date = days_ago(5),
        schedule_interval = '@daily',
        tags = ['weather', 'data'],
        catchup = True
    ) as dag:
        extract_data = PythonOperator(
            task_id = 'extract_weather_data',
            python_callable = main.fetch_weather_data,
            op_kwargs = {'name': 'pker'}
        )
        transform_data = PythonOperator(
            task_id = 'transform_weather_data',
            python_callable = main.transform_weather_data,
            op_kwargs = {'data' : extract_data.output}

            )
        load_data = PythonOperator(
            task_id = 'load_weather_data',
            python_callable = main.load_data
        )

   
    extract_data >> transform_data >> load_data

Next, create a subfolder within dags folder and name it weather_etl. Inside weather_etl subfolder, create two files main.py and __init__.py
The main.py file will contain the logic for retrieving data from the api while weather_dag.py will contain the dag file. Inside weather_dag.py we will import main.py. main.py file contain the following code

    
    import json
    import requests
    import logging
    from airflow.models import Variable
    from datetime import datetime, timedelta, timezone
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    def fetch_weather_data():
        base_host = "api.openweathermap.org"
        endpoint_url = f"http://{base_host}/data/2.5/weather"
        try:
            api_key_value = Variable.get("api_key")
        except Exception as e:
            logging.error(f"Failed to retrieve API key from Airflow Variables 'api_key' : {e}")
            print(f"ERROR: Failed to retrieve API key: {e}")
        city = "Nairobi"
        params = {
            "q": city,
            "appid": api_key_value,
            "units": "metric"
        }

        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()
        data = response.json()
        print("Weather data fetched successfully.")
        return data
    
    def transform_weather_data(data,**kwargs):
        city = data["name"]
        weather_description = data["weather"][0]["description"]
        temperature = data["main"]["temp"] 
        feels_like = data["main"]["feels_like"]
        min_temperature = data["main"]["temp_min"]
        max_temperature = data["main"]["temp_max"]
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]

        tz=timezone(timedelta(seconds = data['timezone']))
                    
        time_recorded = datetime.fromtimestamp(data['dt'], tz=tz)

        sunrise = datetime.fromtimestamp(data['sys']['sunrise'],tz=tz)
        sunset = datetime.fromtimestamp(data['sys']['sunset'],tz=tz)

        transformed_data = {
            "city": city,
            "weather_description": weather_description,
            "temperature": temperature,
            "feels_like": feels_like,
            "min_temperature": min_temperature,
            "max_temperature": max_temperature,
            "pressure": pressure,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "time_recorded": time_recorded.isoformat(),
            "sunrise": sunrise.isoformat(),
            "sunset": sunset.isoformat()
        }
        return [transformed_data]
    
    def load_data(ti):
        records = ti.xcom_pull(task_ids='transform_weather_data')
        
        if not records:
            raise ValueError("No data found in XCom for key 'weather_data'")    
        pg_hook = PostgresHook(postgres_conn_id='my_postgre_conn', schema = 'weather_db')
    
        columns = list(records[0].keys())
        rows = [tuple(record[col] for col in columns) for record in records]
        pg_hook.insert_rows(table='weather_table', rows=rows, target_fields=columns, commit_every=1000, replace=True, replace_index='id')
    
        print(f"Successfully loaded {len(rows)} rows into PostgreSQL.")



    if __name__ == "__main__":
        import os
        from unittest.mock import patch
        SECRET_KEY = os.getenv("api_key_value")

        # Mock the Airflow Variable so it returns a dummy key locally
        with patch("airflow.models.Variable.get") as mock_get:
            mock_get.return_value = SECRET_KEY
            
            print("--- Running local test ---")
            data = fetch_weather_data()
            print(f"City: {data['name']}, Temp: {data['main']['temp']}")

### Connecting Airflow with Postgre PostgreSQL

For airflow to work correctly with PostgreSQL, there are other important configurations needed.
First, install PostgreSQL locally and run the following code to install Postgres on the terminal. 

    pip install apache-airflow-providers-postgres

Once the installation is successful, you will get a prompt to initialize the database as shown below. Type Y and press Enter or run the code below. 

    airflow db init

Now run airflow webserver & if it is not already running. 

    airflow webserver & 

#### 1. Configure Airflow CFG file

Access airflow.cfg file using vim and change the sqlalchemy settings as shown below.

"sql_alchemy_conn = postgresql+psycopg2://postgres:mypassw@172.xx.xx.x:5432/weather_db"

"postgres"  - the username created on PostgreSQL

"mypassw" – Password 

172.xx.xx.x:5432 – IP address of my WSL and port number 5432

weather_db – The name of the database I created on PostgreSQL

#### 2. PostgreSQL installation settings
Access your local installation of PostgreSQL (C:\Program Files\PostgreSQL\17\data) and edit pg_hba.conf and postgresql.conf as follows:

On postgresql.conf make sure on connection settings, the listen_addresses is as shown 

    listen_addresses = '*'

On pg_hba.conf add, the following line on the bottom (open the file as administrator using Notepad) 

    host    all             all             172.0.0.0/8             scram-sha-256 
    
and save the changes. (This is applicable when using WSL to allow all private network addresses) 

Once done restart the PosgresSQL using services.msc on Windows 

#### 3. API-KEY

Go to Airflow UI, Admin > Variables, Create a variable key (name it anything like - api_key) and the actual value should be retrieved from Openweathermap. Then save. 

#### 4. Postgres Connection

Create a Postgres connection on the UI.

Navigate to Admin > Connection > Click + to Add a New Record > specify connection id (my_Postgre_conn), connection type (Postgres), host, username, and password as required. The host should be the Windows Host IP used in sql_alchemy_conn set previously

Save

Once everything is set, run the weather_dag


Query the table in Postgres to show the weather data of the chosen city 











## Airflow Best Practices

### 1. Modularity
While an entire pipeline can be constructed using a single DAG, it is not recommended because of the principle of modularity. Accordingly, every task should perform a single task and be housed within a single DAG to make troubleshooting easier. 
### 2. Determinism 
A deterministic process outputs the same result for a given input. For a DAG run at any given interval, the result should be similar every time. This determinism feature guarantees consistent data.
### 3. Idempotency
It is recommended to overwrite files or use a delete-write pattern when writing data to databases or data warehouses to avoid duplication. A DAG run can run multiple times in a given time interval and if overwrite or delete-write feature is enabled, will output the same set of results as if it were executed once. 

## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://katherineoelsner.com/)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/)
[![twitter](https://img.shields.io/badge/twitter-1DA1F2?style=for-the-badge&logo=twitter&logoColor=white)](https://twitter.com/)


## 🚀 About Me
I'm a data analyst and a data engineer.

Kindly access the project details here https://github.com/Pkerich/Airflow-Weather_Data-Pipeline/tree/main



