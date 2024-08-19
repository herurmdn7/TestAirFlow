'''

Milestone 3

Nama  : Heru
Batch : HCK-018

This program is designed to automate the process of loading CSV data into PostgreSQL, 
then retrieving the data from PostgreSQL for preprocessing to clean and transform it, and finally uploading the processed data into Elasticsearch


'''
# Import Libraries
# Libraries for DAG from airflow
from airflow.models import DAG

# Libraries to execute python function as task in airflow
from airflow.operators.python import PythonOperator

# Libraries for scheduling
from datetime import datetime, timedelta

# Libraries to connect to Postgres
from sqlalchemy import create_engine 

# Libraries for Dataframe
import pandas as pd

# Libraries for Elasticsearch
from elasticsearch import Elasticsearch


def load_csv_to_postgres():
    '''

    Function to Load CSV data into postgres and make a table named "table_m3"

    '''
    # Define PostgreSQL database connection parameters
    database = "airflow_m3" # Name of the PostgreSQL database
    username = "airflow_m3" # Username for database authentication
    password = "airflow_m3" # Password for database authentication
    host = "postgres" # Hostname of the PostgreSQL server (usually 'localhost' or a remote server)

    # Create a PostgreSQL connection URL for SQLAlchemy (using psycopg2 as the driver)
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL to create an SQLAlchemy engine for interacting with the PostgreSQL database
    engine = create_engine(postgres_url)

    # Connect to the PostgreSQL database using the engine
    conn = engine.connect()

    # Read data from the CSV file into a pandas DataFrame
    df = pd.read_csv('/opt/airflow/dags/P2M3_Heru_data_raw.csv')

    # Write the DataFrame to a table in the PostgreSQL database
    df.to_sql('table_m3', conn,  # "table_m3" is the name of the table
              index=False, 
              if_exists='replace')  # 'if_exists="replace"' ensures that if the table already exists, it will be replaced
    

def ambil_data():
    '''
    
    Function to gather data from table_m3 in PostgreSQL. 
    This function includes the necessary information to log in to the PostgreSQL server, including database name, username, password, and host information. 

    '''
    database = "airflow_m3" # Name of the PostgreSQL database
    username = "airflow_m3" # Username for database authentication
    password = "airflow_m3" # Password for database authentication
    host = "postgres" # Hostname of the PostgreSQL server

    # Create a PostgreSQL connection URL for SQLAlchemy (using psycopg2 as the driver)
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL to create an SQLAlchemy engine for interacting with the PostgreSQL database
    engine = create_engine(postgres_url)

    # Connect to the PostgreSQL database using the engine
    conn = engine.connect()

    # Execute an SQL query to select all data from the 'table_m3' table in the database
    df = pd.read_sql_query("select * from table_m3", conn) 

    # Save the retrieved data to a CSV file at the specified location
    df.to_csv('/opt/airflow/dags/P2M3_Heru_data_new.csv',
               sep=',',         # 'sep=","' ensures that the CSV uses commas as the delimiter
               index=False)     # 'index=False' prevents the index from being written to the CSV file
    


def preprocessing(): 
    ''' 
    
    Function to clean the data by removing duplicates, handling missing values, standardizing column names, 
    and adding a unique ID column to fulfill great expectations requirements. 
    
    '''
    
    # Load the data
    data = pd.read_csv("/opt/airflow/dags/P2M3_Heru_data_new.csv")

    # Drop duplicates from dataframe
    data.drop_duplicates(inplace=True)

    # Drop rows with missing values
    data.dropna(inplace=True)
    
    # Convert all column names to lowercase
    data.columns = data.columns.str.lower()

    # Replace spaces with underscores
    data.columns = data.columns.str.replace(' ', '_')

    # Remove any tab and newline characters from the column names
    data.columns = data.columns.str.replace(r'[\t\n]', '')

    # Add a unique 'id' column to the DataFrame, starting from 1
    data['id'] = range(1, len(data) + 1)

    # Create a mapping of locations to ISO 3166-2 codes for kibana map 
    location_to_iso = {
        'Telangana': 'IN-TG',
        'Uttar Pradesh': 'IN-UP',
        'Tamil Nadu': 'IN-TN',
        'Maharashtra': 'IN-MH',
        'Karnataka': 'IN-KA',
        'Bihar': 'IN-BR',
        'West Bengal': 'IN-WB',
        'Madhya Pradesh': 'IN-MP',
        'Chandigarh': 'IN-CH',
        'Delhi': 'IN-DL',
        'Gujarat': 'IN-GJ',
        'Kerala': 'IN-KL',
        'Jharkhand': 'IN-JH',
        'Rajasthan': 'IN-RJ',
        'Haryana': 'IN-HR'
    }

    # Map location names to ISO 3166-2 codes
    data['location_code'] = data['location'].map(location_to_iso)

    # Save cleaned data to a new CSV file
    data.to_csv('/opt/airflow/dags/P2M3_Heru_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    '''

    Function to upload the cleaned CSV data to Elasticsearch.
    It iterates through the rows of the CSV and uploads each row as a document to an Elasticsearch index.

    '''

    # Initialize Elasticsearch instance with the specified host URL
    es = Elasticsearch("http://elasticsearch:9200")

    # Load the cleaned data
    df = pd.read_csv('/opt/airflow/dags/P2M3_Heru_data_clean.csv')
    
    # Loop through each row in the DataFrame
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Heru',  # Owner Name
    'start_date': datetime(2024, 8, 14, 12, 00) # Start Date set
    # - timedelta(hours=7)
}

with DAG(
    "P2M3_heru_DAG_hck", # Project DAG Name
    description='Milestone_3', # Project DAG description
    schedule_interval='30 23 * * *', # 6.30AM schedule interval
    default_args=default_args, # Containts owner and start_date
    catchup=False # Disables catchup (Airflow will not backfill missed runs)
) as dag:
    
    # Task : 1
    '''  Function to load CSV into Postgres.'''
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres', # Identifying task within DAG
        python_callable=load_csv_to_postgres) # Function that loads the CSV into the PostgreSQL database
    
    # Task: 2
    '''  Function to retrieve data from Postgres.'''
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres', # Task for retrieving data from Postgres
        python_callable=ambil_data) # Function that retrieves data from the 'table_m3' in Postgres
    

    # Task: 3
    '''  Function to run prepro function or cleaning data.'''
    edit_data = PythonOperator(
        task_id='edit_data', # Task for data cleaning 
        python_callable=preprocessing) # Function that performs data cleaning and saves the cleaned data

    # Task: 4
    '''  Function to upload cleaned data to Elasticsearch.'''
    upload_data = PythonOperator(
        task_id='upload_data_elastic', # Task for uploading cleaned data to elastic search
        python_callable=upload_to_elasticsearch) # Function that uploads data to an Elasticsearch

    # Set up the order of task execution
    # The workflow is: load_csv_task -> ambil_data_pg -> edit_data -> upload_data
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data


