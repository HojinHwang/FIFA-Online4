from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from pytz import timezone
import psycopg2
import requests
import pandas as pd
import logging
import pytz

KST = pytz.timezone('Asia/Seoul')

# DAG Definition
dag = DAG(
    dag_id='match_record_etl',
    catchup=False,
    start_date=datetime(2024, 8, 15),
    schedule_interval='0 2 * * *'
)

# Function Definition
def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres_airflow_db')
    return hook.get_conn().cursor()

def extract(api_key, ouid):
    logging.info("Extract started")
    headers = {"x-nxopen-api-key": api_key}
    match_url = 'https://open.api.nexon.com/fconline/v1/user/match?'
    match_params = {'ouid': ouid, 'matchtype' : 50, 'offset' : 0, 'limit' : 100}
    response = requests.get(match_url, params=match_params, headers=headers)
    pre_data = response.json()
    logging.info("Extract done")
    return pre_data

def transform(pre_data):
    logging.info("Transform started")
    data = pd.DataFrame(pre_data, columns=['record'])
    data['last_updated'] = datetime.now()
    data['last_updated'] = data['last_updated'].apply(lambda dt: dt.replace(tzinfo=pytz.utc).astimezone(KST))
    logging.info("Transform done")
    return data

def load(data):
    logging.info("Load started")
    cur = get_postgres_connection()
    # from dataframe value to list
    load_data = []
    for idx in data.index:
        record = data.loc[idx, 'record']
        last_updated = data.loc[idx, 'last_updated']
        load_data.append(f"('{record}', '{last_updated}')")
    # delete value from table
    delete_sql = "DELETE FROM fifa4.match;"
    logging.info(delete_sql)
    try:
        cur.execute(delete_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    # insert into value to table
    insert_sql = f"INSERT INTO fifa4.match VALUES" + ",".join(load_data)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise    
    
    logging.info("Load done")

def etl():
    pre_data = extract(api_key, ouid)
    data = transform(pre_data)
    load(data)

# Variables from Airflow
api_key = Variable.get("api_key")
ouid = Variable.get("ouid")

etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl,
    provide_context=True,
    dag=dag
)

# Task Dependencies
etl_task
