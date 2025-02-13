from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.postgres_to_adls_operator import PostgresToADLSOperator
from datetime import datetime, timedelta
import pandas as pd
import csv
import psycopg2 as psy
import numpy as np
import requests


default_args={
    'owner':'harsha',
    'depends_on_past':False,
    'start_date':datetime(2025,2,11)
}

#definging dag
dag=DAG(
    dag_id='airbnb_data_adls_wth_cstm_oprtr',
    default_args=default_args,
    description='This dag loads csv data into postgres sql',
    schedule_interval=timedelta(days=1)
)

listing_dates=['2024-12-07']

# loading data from api endpoint to a csv file
def download_csv():
    listing_url_template="https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{date}/visualisations/listings.csv"
    for date in listing_dates:
        url=listing_url_template.format(date=date)
        response=requests.get(url)
        if response.status_code==200:
            with open(f'/tmp/airbnb_data/listing-{date}.csv','wb') as f:
                f.write(response.content)
        else:
            print(f"failed to download listing for {date}")

download_csv_task=PythonOperator(
    task_id='download_csv',
    python_callable=download_csv,
    dag=dag
)

#processing the data by converting to a DF and again storing in csv file
def process_csv():
    for date in listing_dates:
        input_file=f'/tmp/airbnb_data/listing-{date}.csv'
        output_file=f'/tmp/airbnb_data/listing-{date}-processed.csv'
        df=pd.read_csv(input_file)
        df.fillna('',inplace=True)
        df.to_csv(output_file,index=False,quoting=csv.QUOTE_ALL)

processed_csv_task=PythonOperator(
    task_id='processed_csv',
    python_callable=process_csv,
    dag=dag
)      

# we can directly write the sql query as below or otherwise we can store the command in .sql file
# and pass the file name as the input for the sql. Refer wiki_urlpython.py file
create_table=PostgresOperator(
    task_id='create_table',
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
def load_to_postgres():
    conn=psy.connect("dbname='postgres' user='airflow' host='postgres' password='airflow'")
    cur=conn.cursor()
    for date in listing_dates:
        processed_file=f'/tmp/airbnb_data/listing-{date}-processed.csv'
        with open(processed_file,'r') as f:
            next(f)
            cur.copy_expert("COPY listings FROM stdin WITH CSV HEADER QUOTE '\"' ",f)
        conn.commit()
    cur.close()
    conn.close()

postgres_load_task=PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)    

# Here the custom operator PostgresToADLSOperator intervenes as defined in postgres_to_adls_operator
# and does the fucntionality accordingly

postgres_to_adls_task=PostgresToADLSOperator(
    task_id='postgres_to_ads_task',
    postgres_conn_id='airbnb_postgres',
    adls_conn_id='adls_airbnb',
    sql_query="select * from listings;",
    adls_container='airbnb',
    adls_file_naming='listings_processed',
    dates_list=listing_dates,
    dag=dag
)

download_csv_task >> processed_csv_task >> create_table >> postgres_load_task >> postgres_to_adls_task