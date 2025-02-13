from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from airflow.utils.decorators import apply_defaults
import pandas as pd
import tempfile
import os

# This custom operator was created to load data from PostgreSQL to ADLS
# As there is no direct module for this operation, we use PostgresHook to connect to Postgres from
# Airflow environment and AzureDataLakeStorageV2Hook to connect to ADLS from Airflow environment

class PostgresToADLSOperator(BaseOperator):
    @apply_defaults
    def __init__(self,adls_conn_id,postgres_conn_id,sql_query,adls_container,adls_file_naming,dates_list,*args,**kwargs):
        
        super().__init__(*args,**kwargs)
        self.adls_conn_id=adls_conn_id
        self.postgres_conn_id=postgres_conn_id
        self.sql_query=sql_query
        self.adls_container=adls_container
        self.adls_file_naming=adls_file_naming
        self.dates_list=dates_list

#we were reading the data from PostgreSQL table and storing in DF, but AzureDataLakeStorageV2Hook doesn't
# support direct upload from DF, so using temporary file to store the data

    def execute(self,context):
        postgres_hook=PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn=postgres_hook.get_conn()              #can also build the connect by psycopg2.connect()
        cur=conn.cursor()
        cur.execute(self.sql_query)
        rows=cur.fetchall()                                #cursor stores every row as a tuple
        cols=[desc[0] for desc in cur.description]         #reading the columns
        df=pd.DataFrame(rows,columns=cols)
        tmp_file=tempfile.NamedTemporaryFile(delete=False,suffix='.csv')
        df.to_csv(tmp_file.name,index=False)
        for date in self.dates_list:
            try:
                adls_hook=AzureDataLakeStorageV2Hook(adls_conn_id=self.adls_conn_id)
                adls_hook.upload_file(
                    file_system_name=self.adls_container,
                    file_name=f"{self.adls_file_naming}_{date}.csv",
                    file_path=tmp_file.name,
                    overwrite=True)
            except Exception as e:
                print(f'Raised exception as {e}')
        os.remove(tmp_file.name)
        cur.close()
        conn.close()

