import requests
from airflow.operators.python import PythonOperator
from airflow import DAG
import os
import datetime
from dotenv import load_dotenv
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 3, 24),
    'retries': 1,
}

def extract():
    """
    make api call to football-data
    """
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    API_AUTH_TOKEN = os.getenv('FOOTBALL_DATA_KEY')
    print("token " + str(API_AUTH_TOKEN))
    url = "http://api.football-data.org/v4/competitions/2014/scorers"
    headers = {'X-Auth-Token': API_AUTH_TOKEN}

    response = requests.get(url, headers=headers)
    print(response)
    if response.status_code == 200:
        return response.json()  # Return JSON data from the API
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def transform(**kwargs):
    """
    Transform data into what is required for table
    """
    data = kwargs['ti'].xcom_pull(task_ids='extract')
    scorers_data = data['scorers']
    df = pd.DataFrame(scorers_data)

    df = df[['player', 'team', 'goals', 'assists', 'penalties', 'playedMatches']].fillna(0)

    df['player'] = df['player'].apply(lambda x: x['name'])
    df['team'] = df['team'].apply(lambda x: x['name'])

    print(df)
    return df

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform')
    csv_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'top_scorers.csv')

    data.to_csv(csv_file_path, index=False)

dag = DAG(
    'update_top_scorers_csv',
    default_args=default_args,
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
