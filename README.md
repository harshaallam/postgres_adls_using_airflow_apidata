# APi Data to ADLS using Airflow and PostgreSQL as staging 

This project uses Apache Aiflow to automate the flow of loading of API data to Azure Data Lake Storage by using PostgreSQL as stager.

- I used the Amsterdam listings.csv data by pulling the data from  API endpoint https://insideairbnb.com/get-the-data/
- Create a DAG file and use the PythonOperator to download the csv file from the endpoint. Later prepocess the csv file using the DataFrame.
- Create a table with schema in PostgreSQL which was already set up in the docker-compose.yaml file.
- Get the container-id by uisng the command 'docker ps' and get into the bash of the postgresql container by using docker exec -it <cont-id> bash
- Connect to the PostgreSQL by using psql -U <user-id> -d <database>.
- Check for the successful table creation in the database.
- Then load the data into PostgreSQL table using cursor.compy.expert(). This uses instead of INSERT commands for bulk loading.
- Now, to load the data in csv format into ADLS is the task. As there is not module to load directly from PostgreSQL to ADLS.
- Here, I've approache two methods to achieve the taks. One is by creating a custom operator by uisng PostgresHook, and AzureDataLakeStorageV2Hook. Refer the code file `airbnb_postgres_usng_cstm_operator.py` and custom operator `postgres_to_adls_operator.py`. Place the custom operator code in  `pluggins/` by adding `__init__.py` file in the same directory.
- Second one is by using Xcom to share the data between tasks in the Airflow dag. Refer `airbnb_postgres_usng_wthout_cstm_operator.py` file.
- 
