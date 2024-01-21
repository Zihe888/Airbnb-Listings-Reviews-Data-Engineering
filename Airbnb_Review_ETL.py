from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import json

def extract_data(**kwargs):
    
    ## read csv file
    ## apache airflow doesn't support interactive use
    ## the file path should be dockers' filepath, but we can create bind mounts
    file_name = 'Reviews.csv'
    file_path = '/opt/airflow/dags/data/'
    file = file_path + file_name
    df = pd.read_csv(file, encoding='ISO-8859-1')
    # Convert DataFrame to a JSON-serializable format
    df_json = df.to_json(orient='split')

    # Push the JSON-serializable data to XCom
    kwargs['ti'].xcom_push(key='loaded_data', value=df_json)


def proccess_reviews(**kwargs):
    # Pull data from XCom
    json_data = kwargs['task_instance'].xcom_pull(task_ids="load_reviews", key='loaded_data')
    df = pd.read_json(json_data, orient='split')
    df = df.replace(np.nan, None)

    ## choose columns to build dataframe ##
    selected_columns = ['listing_id', 'date', 'reviewer_id', 'reviewer_name', 'comments']
    Listsings_df = df[selected_columns]
    
    ## Remove duplicates ##
    duplicates_exist = Listsings_df.duplicated()
    if duplicates_exist.any():
        Listsings_df.drop_duplicates(subset=selected_columns, inplace=True)

    ### crate dict to store what columns should be included in MongoDB
    Mongodata = {}
    Mongodata['hotel_review_text'] = selected_columns
    
    # Convert Mongodata to a JSON-serializable format
    Mongodata_json = json.dumps(Mongodata)

    # Push the DataFrame and Mongodata to XCom
    kwargs['ti'].xcom_push(key='loaded_data', value=Listsings_df.to_json(orient='records'))
    kwargs['ti'].xcom_push(key='mongodata', value=Mongodata_json)


#def load_reviews_mongodb(Mongodata_name, **kwargs):
def load_reviews_mongodb(**kwargs):
    
    ## Prepare data ##
    Mongodata_name = 'hotel_review_text'
    # Pull the JSON-serialized DataFrame from XCom
    df_json = kwargs['ti'].xcom_pull(key='loaded_data')

    # Convert JSON to DataFrame
    Listsings_df = pd.read_json(df_json, orient='records')

    # Pull and deserialize Mongodata
    mongodata_json = kwargs['ti'].xcom_pull(key='mongodata')
    Mongodata = json.loads(mongodata_json)

    # Listsings_df, Mongodata = kwargs['task_instance'].xcom_pull(task_ids="proccess_reviews")
    column_name = Mongodata[Mongodata_name]
    Listsings_df = Listsings_df[column_name]

    ## Combine 'host_id' and 'host_about' into a new column 'host' ##
    Listsings_df['listing_id'] = pd.to_numeric(Listsings_df['listing_id'], errors='coerce')

    # delete rows with 'NaN' value in 'listing_id' field
    Listsings_df = Listsings_df.dropna(subset=['listing_id'])

    Listsings_df['reviews'] = Listsings_df.apply(lambda row: {'date': row['date'], 'reviewer_id': row['reviewer_id'], 'reviewer_name': row['reviewer_name'], 'comments': row['comments']}, axis=1)

    ## Drop the original 'host_id' and 'host_about' columns ##
    Listsings_df = Listsings_df.drop(['date', 'reviewer_id', 'reviewer_name', 'comments'], axis=1)
    Listsings_df = Listsings_df.dropna()
    Listsings_df = Listsings_df.to_dict(orient='records')

    ## create mongodbhook ##
    with MongoHook(conn_id='airflow_mongo') as mongo_hook:
        collection_name = "airbnb_hotel_info"

        # Iterate through each document
        for document in Listsings_df:
            # Extract the 'reviews' field from the document
            reviews_entry = document.get('reviews', [])

            # If there are new entries, perform the update
            if reviews_entry:
                id = int(document['listing_id'])
                filter_criteria = {'id': id}
                update_operation = {'$addToSet': {'reviews': reviews_entry}}
                    
                try:
                    ## modify part of the document
                    result = mongo_hook.update_one(
                        mongo_collection = collection_name,
                        filter_doc=filter_criteria, 
                        update_doc=update_operation,
                        mongo_db='airbnb', 
                        upsert=True)       
                    
                    if result.modified_count > 0 or result.upserted_id is not None:
                        print("Update successful")
                except Exception as e:
                    print(f"Error updating document with id {document['id']}: {e}")
                
file_name2 = 'Reviews.csv'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'timezone': 'PST'
}

with DAG(
    "Airbnb_Review_ETL",
    default_args=default_args,
    description="conduct ETL process of Airbnb Reviews data",
    schedule=timedelta(days=1), ## run this dag everyday
) as dag_reviews:
    
    start_task = EmptyOperator(task_id="ETL", dag=dag_reviews)

    extract_review_task = PythonOperator(
        task_id="load_reviews",
        python_callable=extract_data,
        #op_kwargs={'file_name': file_name2},
        provide_context=True,
        dag=dag_reviews,
    )

    proccess_reviews_task = PythonOperator(
        task_id="proccess_reviews",
        python_callable=proccess_reviews,
        provide_context=True,
        #do_xcom_push=True,
        dag=dag_reviews,
    )  

    # mongodata_name_review = 'hotel_review_text'
    load_reviews_mongo_task = PythonOperator(
        task_id="load_reviews_mongodb",
        python_callable=load_reviews_mongodb,
        #op_args={'Mongodata_name':'hotel_review_text'},
        #provide_context=True,
        #do_xcom_push=True,
        dag=dag_reviews,       
    )

start_task >> extract_review_task >> proccess_reviews_task >> load_reviews_mongo_task