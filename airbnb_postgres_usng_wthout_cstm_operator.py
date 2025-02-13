from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.postgres_to_adls_operator import PostgresToADLSOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime, timedelta
import pandas as pd
import csv
import psycopg2 as psy
import numpy as np
import tempfile
import requests

# This  works in the same way as airbnb_postgres_usng_cstm_oprtr.py. But it uses custom operator 
# postgres_to_adls_operator to accomplish the task
# this code file does the same thing, but uses Xcom to share the data between tasks


default_args={
    'owner':'harsha',
    'depends_on_past':False,
    'start_date':datetime(2025,2,13)
}

dag=DAG(
    dag_id='airbnb_PostgresToADLS_wout_cstm_oprator',
    default_args=default_args,
    description='This dag loads data from the api to adls by storing intermittently in postgres',
    schedule_interval=timedelta(days=1)
)

listing_dates=['2024-12-07']

# loading data from api endpoint to a csv file
def download_csv():
    for date in listing_dates:
        url_template=f"https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{date}/visualisations/listings.csv"
        url=url_template.format(date=date)
        response=requests.get(url)
        if response.status_code==200:
            with open(f"/tmp/airbnb_data/listing_raw_{date}.csv",'wb') as f:
                f.write(response.content)
        else:
            print('File is not available')

download_csv_taks=PythonOperator(
    task_id='download_csv',
    python_callable=download_csv,
    dag=dag
)

#processing the data by converting to a DF and again storing in csv file
def preprocess_csv():
    for date in listing_dates:
        df=pd.read_csv(f"/tmp/airbnb_data/listing_raw_{date}.csv")
        df.fillna('',inplace=True)
        df.to_csv(f"/tmp/airbnb_data/listing_processed_{date}.csv",index=False,quoting=csv.QUOTE_ALL)

preprocess_csv_task=PythonOperator(
    task_id='preprocess_csv_task',
    python_callable=preprocess_csv,
    dag=dag
)

# we can directly write the sql query as below or otherwise we can store the command in .sql file
# and pass the file name as the input for the sql. Refer wiki_urlpython.py file
create_table_task=PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='airbnb_postgres',
    sql="""
    DROP TABLE listings;
    CREATE TABLE IF NOT EXISTS listings (
    ⁠id BIGINT, 
    name TEXT, 
    host_id INTEGER,
    host_name VARCHAR (100),
    neighbourhood_group VARCHAR(100),
    neighbourhood VARCHAR(100), 
    Latitude NUMERIC (18, 16), 
    longitude NUMERIC (18,16), 
    room_type VARCHAR (100), 
    price VARCHAR(100), 
    minimum_nights INTEGER, 
    number_of_reviews INTEGER, 
    last_review VARCHAR(100), 
    reviews_per_month VARCHAR(100), 
    calculated_host_listings_count INTEGER, 
    availability_365 INTEGER, 
    number_of_reviews_ltm INTEGER, 
    license VARCHAR(100)
    );
    """,
    dag=dag
)

# load data into postgres RDBMS table using cur.copy_expert(), this is used for bulk loading of data 
# rather than INSERT command
def load_data_postgres():
    conn=psy.connect(dbname='postgres', host='postgres', user='airflow', password='airflow')
    cur=conn.cursor()
    for date in listing_dates:
        with open(f"/tmp/airbnb_data/listing_processed_{date}.csv",'r') as f:
            next(f)
            cur.copy_expert("COPY listings FROM stdin WITH CSV HEADER QUOTE '\"' ",f)
        conn.commit()
    cur.close()
    conn.close()

load_postgres_task=PythonOperator(
    task_id='load_postgres_task',
    python_callable=load_data_postgres,
    dag=dag
)

#we were reading the data from PostgreSQL table and storing in DF, but AzureDataLakeStorageV2Hook doesn't
# support direct upload from DF, so using temporary file to store the data. Therefore the tmp_file is shared
# with the help of Xcom between the tasks.
def read_from_postgres(**kwargs):
    conn=psy.connect(dbname='postgres', host='postgres', user='airflow', password='airflow')
    cur=conn.cursor()                               #can also build the connect by psycopg2.connect()
    cur.execute("select * from listings;")
    rows=cur.fetchall()                                   #cursor stores every row as a tuple
    cols=[desc[0] for desc in cur.description]              #reading the columns
    df=pd.DataFrame(rows,columns=cols)
    tmp_file=tempfile.NamedTemporaryFile(delete=False,suffix='.csv')
    df.to_csv(tmp_file.name,index=False)
    kwargs['ti'].xcom_push(key='tmpfile',value=tmp_file.name)
    cur.close()
    conn.close()

read_postgres_task=PythonOperator(
    task_id='read_postgres_task',
    python_callable=read_from_postgres,
    dag=dag
)

def load_to_adls(**kwargs):
    tmp_file_path=kwargs['ti'].xcom_pull(key='tmpfile',task_ids='read_postgres_task')
    adls_hook=AzureDataLakeStorageV2Hook(adls_conn_id=kwargs['templates_dict']['conn_id'])
    for date in listing_dates:
        try:
            adls_hook.upload_file(
                file_system_name=kwargs['templates_dict']['adls_cont'],
                file_name=f"{kwargs['templates_dict']['adls_file_naming']}_{date}.csv",
                file_path=tmp_file_path,
                overwrite=True)
        except Exception as e:
            print(f"Raised with exception {e}")

load_to_adls_task=PythonOperator(
    task_id='load_to_adls_task',
    python_callable=load_to_adls,
    templates_dict={
        'conn_id':'adls_airbnb','adls_cont':'airbnb','adls_file_naming':'listings_processed'
        },
    dag=dag
)      


download_csv_taks >> preprocess_csv_task >> create_table_task >> load_postgres_task >> read_postgres_task >> load_to_adls_task
    



