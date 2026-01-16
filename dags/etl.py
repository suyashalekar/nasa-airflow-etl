from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator # Use HttpOperator instead
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from datetime import datetime
import json 


## Define Dags
with DAG(
    dag_id ="nasa_api_postgres",
    start_date=datetime(2025, 1, 1), # Use a fixed date in the past,
    schedule = '@daily',
    catchup = False
) as dag:
    
    ## Step 1 : Create the table 
    @task 
    def create_table():

        postgreshook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        create_table_query = """
        
        CREATE TABLE IF NOT EXISTS nasa_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        postgreshook.run(create_table_query)
        
#https://api.nasa.gov/planetary/apod?api_key=hTJs7aWD10LvMkvAzJhPXloOCn1dumoM88BVEe0d
    extract_apod = HttpOperator(
        task_id = "extract_api",
        http_conn_id = 'nasa_api',
        endpoint = 'planetary/apod',
        method = 'GET',
        data = {"api_key" :"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter = lambda response:response.json(),

    )

    @task
    def transform_nasa_data(response):
        nasa_data={
            'title' : response.get('title',''),
            'explanation' : response.get('explanation',''),
            'url' : response.get('url',''),
            'date' : response.get('date',''),
            'media_type' : response.get('media_type','')
        }
        return nasa_data

    @task
    def load_data_to_postgres(nasa_data):

        postgreshook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        insert_query = """
        INSERT INTO nasa_data (title, explanation, url, date, media_type)
        VALUES (%s,%s, %s, %s, %s);
        """

        postgreshook.run(insert_query,parameters = (
            nasa_data['title'], 
            nasa_data['explanation'],
            nasa_data['url'],
            nasa_data['date'],
            nasa_data['media_type'],
        ))
        

    create_table() >> extract_apod
    api_response = extract_apod.output

    transformed_data = transform_nasa_data(api_response)

    load_data_to_postgres(transformed_data)