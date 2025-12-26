
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
    

