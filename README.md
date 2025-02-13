# API Data to ADLS Using Airflow and PostgreSQL as Staging

This project automates the process of loading API data into **Azure Data Lake Storage (ADLS)** using **Apache Airflow** and **PostgreSQL** as a staging layer.

---

## **Project Overview**
The workflow involves:
1. Extracting data from an API endpoint.
2. Staging the data in a PostgreSQL database.
3. Loading the data into Azure Data Lake Storage (ADLS) in CSV format.

The project demonstrates two approaches:
1. Using a **custom Airflow operator** to transfer data from PostgreSQL to ADLS.
2. Using **XCom** to share data between tasks in the Airflow DAG.

---

## **Setup Instructions**

### **1. Airflow and PostgreSQL Setup**
- Use the provided `docker-compose.yaml` file to set up Apache Airflow and PostgreSQL in Docker.
- Mount the local storage to the environment variable `${AIRFLOW_PROJ_DIR:-.}`. This typically points to the directory where the `docker-compose.yaml` file is located.
- Access the services:
  - **Apache Airflow**: Open `http://localhost:8080` in your browser.
  - **PostgreSQL**: Use the command `docker exec -it <container-id> bash` to access the PostgreSQL container.

### **2. Data Source**
- The project uses the **Amsterdam Listings** dataset, which is fetched from the API endpoint: [Inside Airbnb - Get the Data](https://insideairbnb.com/get-the-data/).

---

## **Implementation Steps**

### **1. Data Extraction**
- Create an Airflow DAG file and use the `PythonOperator` to:
  - Download the CSV file from the API endpoint.
  - Preprocess the CSV file using a Pandas DataFrame.

### **2. PostgreSQL Staging**
- Create a table in PostgreSQL with the required schema.
- Access the PostgreSQL container using:
  ```bash
  docker exec -it <container-id> bash
  psql -U <user-id> -d <database>

### **3. Data Loading to ADLS**
Since there is no direct module to load data from PostgreSQL to ADLS, two approaches are implemented:

#### **Approach 1: Custom Operator**
- Create a custom operator using `PostgresHook` and `AzureDataLakeStorageV2Hook`.
- Place the custom operator code in the `plugins/` directory and include an `__init__.py` file.
- Use `PostgresHook` to connect to PostgreSQL and `AzureDataLakeStorageV2Hook` to connect to ADLS.
- Store the data temporarily as a CSV file using the `tempfile` module before uploading it to ADLS.

#### **Approach 2: XCom for Data Sharing**
- Use **XCom** to share data between tasks in the Airflow DAG.
- Similar to the custom operator approach, use the `tempfile` module to create an intermediate CSV file before uploading it to ADLS.

---

## **Code Files**
- **Custom Operator Approach**:
  - DAG File: `airbnb_postgres_usng_cstm_operator.py`
  - Custom Operator: `plugins/postgres_to_adls_operator.py`
- **XCom Approach**:
  - DAG File: `airbnb_postgres_usng_wthout_cstm_operator.py`

---

## **Key Features**
- **Automated Workflow**: The entire process of data extraction, staging, and loading is automated using Apache Airflow.
- **Scalable Staging**: PostgreSQL is used as a staging layer for efficient data processing.
- **Flexible Data Loading**: Two approaches (custom operator and XCom) are implemented to load data into ADLS.
- **Error Handling**: The workflow includes error handling and logging for robust execution.

---

## **How to Run**
1. Clone the repository.
2. Set up the environment using the provided `docker-compose.yaml` file.
3. Place the DAG files in the `dags/` directory and the custom operator in the `plugins/` directory.
4. Start the Airflow server and trigger the DAG.

---

## **Future Enhancements**
- Add support for additional data sources and formats.
- Implement data validation and quality checks.
- Optimize the data loading process for large datasets.

---

## **References**
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Azure Data Lake Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Inside Airbnb - Get the Data](https://insideairbnb.com/get-the-data/)
