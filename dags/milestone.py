from airflow.models import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd

from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk

def load_csv_to_postgres():
 
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_Heru_data_raw.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('table_m3', conn, 
              index=False, 
              if_exists='replace')  # M
    

def ambil_data():
    '''
    
    '''
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_Heru_data_raw_new.csv', sep=',', index=False)
    


def preprocessing(): 
    ''' Function to clean data '''
    
    # Load the data
    data = pd.read_csv("/opt/airflow/dags/P2M3_Heru_data_raw.csv")

    # Drop duplicates
    data.drop_duplicates(inplace=True)

    # Drop rows with missing values
    data.dropna(inplace=True)

    # Clean column names step by step
    
    # Step 1: Convert all column names to lowercase
    data.columns = data.columns.str.lower()
    # This ensures all column names are in lowercase to avoid case-sensitivity issues.

    # Step 2: Replace spaces with underscores
    data.columns = data.columns.str.replace(' ', '_')
    # This replaces any spaces in the column names with underscores, making them more uniform and easier to reference.

    # Step 3: Remove any tab (\t) and newline (\n) characters from the column names
    data.columns = data.columns.str.replace(r'[\t\n]', '')
    # This removes any accidental tab or newline characters from column names, which could have been introduced during data entry.

    
    # Save cleaned data to a new CSV file
    data.to_csv('/opt/airflow/dags/P2M3_Heru_data_raw_clean.csv', index=False)
    
def upload_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_Heru_data_raw_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Heru', 
    'start_date': datetime(2024, 8, 14, 12, 00) 
    # - timedelta(hours=7)
}

with DAG(
    "P2M3_heru_DAG_hck", #atur sesuai nama project kalian
    description='Milestone_3',
    schedule_interval='30 23 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    
    # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi yang kalian buat
    
    #task: 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 4
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data


