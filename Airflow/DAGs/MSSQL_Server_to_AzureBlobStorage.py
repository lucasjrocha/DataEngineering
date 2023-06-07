from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pandas as pd
import logging
import tempfile

logger = logging.getLogger(__name__)
# Reading data from on-premise MSSQL Server database and saving it into a Pandas dataframe
def get_data_from_sql():
    mssql_hook = MsSqlHook(mssql_conn_id='MSSQL_CONNECTION') # The connectino you've created in Airflow Connections
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    logger.info("Querying data.")

    # Execute SQL query
    cursor.execute('SELECT TOP(3000000) * FROM DataBase.Schema.Table')
    #print(cursor.description)

    # Fetch all rows
    rows = cursor.fetchall()
    logger.info("Loading data into a Pandas dataframe")

    # Convert rows to DataFrame
    df = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return df

def az_upload():
    AZURE_CONN_ID = "adls-blob" # The connection you've created in Airflow Connections

    az_hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    logging.info("Exporting SQL data to Azure Blob Storage")
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = get_data_from_sql()
        logger.info("TempFile")
        df.to_parquet(f"{tmpdirname}/TableName.parquet")
        logger.info("Writing data into the Azure Blob Storage / Data Lake")
        az_hook.load_file(
            file_path=f"{tmpdirname}/TableName.parquet",
            container_name="ContainerName",
            blob_name="airflow/MSSQLtoAzureBlobStorage/TableName.parquet" # Path and archive name
            overwrite=True
        )
        logger.info("Data written successfully!!")

default_args = {
    'owner': 'TheOwnerName',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="azure_upload_five",
    schedule_interval='5 4 * * *',
    catchup=False,
    tags=['localtoazure', 'azure']
) as dag:

    upload = PythonOperator(
        task_id="upload_azure_datalake",
        python_callable=az_upload
    )

    upload
