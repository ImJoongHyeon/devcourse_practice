import requests
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
# from pandas import Timestamp

import logging
import psycopg2
    
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    rs = requests.get(url)
    response_info = {'data' : {}, 'check': True}
    
    if rs.status_code == 200:
        response_info["data"] = rs.json()
    else:
        response_info["check"] = False
    
    return response_info

@task
def transform(response_info):
    records = []
    if response_info["check"] == True:
        for l in response_info["data"]:
            country_name = l["name"]["official"]
            population = l["population"]
            area = l["area"]
            records.append([country_name, population, area])
        
    return records

@task
def load(schema, table, records):
    logging.info("load started") 
    cur = get_Redshift_connection()
    """
    records = [
        [country, population, area]
    ]
    """
    
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # FULL REFRESH
        for r in records:
            country_name = r[0]
            population = int(r[1])
            area = float(r[2])
            print('country : ' + country_name, ' population : ' + str(population), ' area : ' + str(area))
            sql = f"INSERT INTO {schema}.{table} VALUES ('{country_name}', '{population}', '{area}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
    logging.info("load done")

with DAG(
    dag_id = 'WorldwideCountryInfo',
    start_date = datetime(2024,5,1),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:

    url = Variable.get("country_info_url")
    schema = "joongh0113"
    table = "worldwide_country_info"
    
    load(schema, table, transform(extract(url)))
