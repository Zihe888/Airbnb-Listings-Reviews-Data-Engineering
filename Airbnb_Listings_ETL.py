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


## define functions ##
def extract_list_data(file_name):
    
    ## read csv file
    ## apache airflow doesn't support interactive use
    ## the file path should be dockers' filepath, but we can create bind mounts
    ## file_name = 'Listings.csv'
    file_path = '/opt/airflow/dags/data/'
    file = file_path + file_name
    df = pd.read_csv(file)
    # print(df)
    # df = df.replace(np.nan, None)
    ## The xcom_pull operation might be treating None as NaN during retrieval so there's no need to replace nan with None at this stage
    return df

def proccess_lists(**kwargs):

    df = kwargs['task_instance'].xcom_pull(task_ids="extract_lists_task")
    #print(df)
    ## NaN -> None ##
    ### if I don't do this step, it may cause problem when loading data into postgres database
    ### because integer column doesn't accept 'NaN'::float. It will cause integer out of range problem
    ### however, it accept None value.
    df = df.replace(np.nan, None)

    ## choose columns to build dataframe ##
    selected_columns = ['id', 'listing_url', 'name', 'summary', 'space', 'description', 'neighborhood_overview', 'notes', 'transit', 'host_id', 'host_url', 'host_name', 'host_since', 'host_location', 'host_about', 'host_response_time', 'host_response_rate', 'host_acceptance_rate', 'host_neighbourhood', 'host_listings_count', 'host_total_listings_count', 'host_verifications', 'street', 'neighbourhood', 'city', 'state', 'zipcode', 'market', 'smart_location', 'latitude', 'longitude', 'property_type', 'room_type', 'accommodates', 'bathrooms', 'bedrooms', 'beds', 'bed_type', 'amenities', 'square_feet' , 'price' , 'weekly_price', 'monthly_price', 'security_deposit', 'cleaning_fee', 'guests_included', 'extra_people', 'minimum_nights', 'maximum_nights', 'calendar_updated', 'availability_30', 'availability_60', 'availability_90', 'availability_365', 'requires_license', 'license', 'jurisdiction_names', 'cancellation_policy', 'require_guest_profile_picture', 'require_guest_phone_verification', 'calculated_host_listings_count', 'reviews_per_month']
    Listsings_df = df[selected_columns]
    
    ## modify the column name ##
    Listsings_df.rename(columns={'neighbourhood': 'neighborhood'}, inplace=True)
    
    ## Remove duplicates ##
    duplicates_exist = Listsings_df.duplicated(subset='id')
    if duplicates_exist.any():
        Listsings_df.drop_duplicates(subset='id', inplace=True)

    ## Data Transformation ##
    ### delete '$' ###
    columns = ['price', 'weekly_price', 'monthly_price', 'security_deposit', 'cleaning_fee', 'guests_included', 'extra_people']
    for column in columns:
        Listsings_df[column] = Listsings_df[column].astype(str).str.replace('$', '')
    
    ### zipcode data 20009-374 => 20009 ###
    for i in range(len(Listsings_df['zipcode'])):
        if len(str(Listsings_df['zipcode'][i])) > 5:
            Listsings_df['zipcode'][i] = str(Listsings_df['zipcode'][i])[:5]

    ### state ###
    for i in range(len(Listsings_df['state'])):
        if str(Listsings_df['state'][i]) == 'Washington DC':
            Listsings_df['state'][i] = 'DC'

    ### City ###
    for i in range(len(Listsings_df['city'])):
        if str(Listsings_df['city'][i]) == 'Washington, D.C.':
            Listsings_df['state'][i] = 'DC'       

    ### drop rows that name column is nan ###
    Listsings_df = Listsings_df.dropna(subset=['name'])
    
    ### create dict to store what columns should be included in each table
    tableSchema = {}
    tableSchema['host_info'] = ['id', 'host_id', 'host_url', 'host_name', 'host_since', 'host_location', 'host_response_time', 'host_response_rate', 'host_acceptance_rate', 'host_neighbourhood', 'host_listings_count', 'host_total_listings_count', 'host_verifications']
    tableSchema['hotel_location'] = ['id', 'street', 'neighborhood', 'city', 'state', 'zipcode', 'market', 'smart_location', 'latitude', 'longitude']
    tableSchema['hotel_facilities'] = ['id', 'property_type', 'room_type', 'accommodates', 'bathrooms', 'bedrooms' , 'beds', 'bed_type', 'amenities', 'square_feet']
    tableSchema['price_info'] = ['id', 'price', 'weekly_price', 'monthly_price', 'security_deposit', 'cleaning_fee', 'guests_included', 'extra_people', 'minimum_nights', 'maximum_nights', 'calendar_updated', 'availability_30', 'availability_60', 'availability_90', 'availability_365']
    tableSchema['host_metrics'] = ['id', 'requires_license', 'license', 'jurisdiction_names', 'cancellation_policy', 'require_guest_profile_picture', 'require_guest_phone_verification', 'calculated_host_listings_count', 'reviews_per_month']
    
    ### crate dict to store what columns should be included in MongoDB
    Mongodata = {}
    Mongodata['hotel&host_text'] = ['id','listing_url','name','host_id','summary','space','description','neighborhood_overview','notes','transit','host_about']
    
    return Listsings_df, tableSchema, Mongodata

### need to be executed many times(depend on how many tables are there) ###
def load_list_postgres(table_name, **kwargs):

    Listsings_df, tableschema, _ = kwargs['task_instance'].xcom_pull(task_ids="proccess_lists")
    # table_name = 'host_metrics'
    if table_name == 'hotel_facilities':
        Listsings_df = Listsings_df.fillna(-1)

    column_name = tableschema[table_name]
    Listsings_df = Listsings_df[column_name]

    ## final data ##
    ### columns ###
    columns = []
    for column in Listsings_df.columns:
        columns.append(column)

    ### row_values ### 
    rows = []
    for _, row in Listsings_df.iterrows():
        row_values = tuple(row)
        rows.append(row_values)

    ## we can use the following function to connect postgres database ##
    ## However, please make sure that the postgres database share the same network with airflow!!!! 
    ## otherwise, there will definitely throw an error like can not connect database or cannot find it
    '''
    def connect_to_postgresql():
        # connect PostgreSQL database
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="500b473bd586",
            #host="my_postgres",
            port="5432"
        )
        return conn
    '''
    '''
    with PostgresHook(postgres_conn_id='my_postgres') as postgres_hook:
    
        postgres_hook.insert_rows(
                #table='airbnb_listings',
                table = table_name,
                rows = rows,
                target_fields = columns,
                upsert = True,
                replace = True,
                replace_index = 'id',
        )
    '''
    postgres_hook = PostgresHook(postgres_conn_id='my_postgres')
    
    postgres_hook.insert_rows(
            #table='airbnb_listings',
            table = table_name,
            rows = rows,
            target_fields = columns,
            upsert = True,
            replace = True,
            replace_index = 'id',
    )

#def load_lists_mongo(Mongodata_name, **kwargs):
def load_lists_mongo(**kwargs):   
    ## Prepare data ##
    Listsings_df, _, Mongodata = kwargs['task_instance'].xcom_pull(task_ids="proccess_lists")
    Mongodata_name = "hotel&host_text"
    column_name = Mongodata[Mongodata_name]
    Listsings_df = Listsings_df[column_name]

    ## Combine 'host_id' and 'host_about' into a new column 'host' ##
    Listsings_df["host_desc"] = Listsings_df.apply( lambda row: {"host_id": row["host_id"], "host_about": row["host_about"]}, axis=1)

    ## Drop the original 'host_id' and 'host_about' columns ##
    elements_to_drop = ['host_id', 'host_about']
    Listsings_df = Listsings_df.drop(elements_to_drop, axis=1)
    Listsings_df = Listsings_df.dropna()
    Listsings_df = Listsings_df.to_dict(orient='records')
    

    ## create mongodbhook ##
    with MongoHook(conn_id='airflow_mongo') as mongo_hook:
        client = mongo_hook.get_conn()
        db = client.airbnb
        collection_name = "airbnb_hotel_info"
        db[collection_name].create_index("id", unique=True)
        
        for List in Listsings_df:
            
            if List:
                # host_desc = List.get('host_desc', [])
                filter_criteria = {
                    "id": List["id"],
                    "listing_url": List["listing_url"]
                }
                '''
                update_operation = {
                    '$addToSet': {'host_desc': List["host_desc"]},
                    '$set': {
                        "name": List["name"],
                        "summary": List["summary"],
                        "space": List["space"],
                        "description": List["description"],
                        "neighborhood_overview": List["neighborhood_overview"],
                        "notes": List["notes"],
                        "transit": List["transit"]
                    }
                }
                '''
                ## update data ##   
                mongo_hook.replace_one(
                    mongo_collection = collection_name,
                    filter_doc=filter_criteria,
                    doc=List,
                    mongo_db='airbnb',
                    upsert=True
                )
            
                    
## input data ##
file_name1 = 'Listings.csv'
## sql ##
#### create table schema ####

## define a dag ##
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'timezone': 'PST'
}

## dag_listings ## Load data from csv file to MongoDB and PostgreSQL
with DAG(
    "Airbnb_Listings_ETL",
    default_args=default_args,
    description="conduct ETL process of Airbnb Listings data",
    schedule=timedelta(days=1), ## run this dag everyday
) as dag_listings:

    ## define tasks ##
    start_task = EmptyOperator(task_id="ETL", dag=dag_listings)

    extract_lists_task = PythonOperator(
        task_id="extract_lists_task",
        op_kwargs={'file_name': file_name1},
        python_callable=extract_list_data,
        #provide_context=True,
        dag=dag_listings,
    )

    Proccess_lists = PythonOperator(
        task_id="proccess_lists",
        python_callable=proccess_lists,
        #provide_context=True,
        #do_xcom_push=True,
        dag=dag_listings,
    )

    # create tables if not exist
    create_table_dict = {}
    create_table_tasks = ['create_host_info','create_hotel_location', 'create_hotel_facilities', 'create_price_info', 'create_host_metrics']    
    for task in create_table_tasks:
        sql_name = 'sql/' + task + '.sql'
        create_table_task = PostgresOperator(
            task_id=task,
            sql=sql_name,
            postgres_conn_id='my_postgres',  # Set your PostgreSQL connection ID
            autocommit=True,
            dag=dag_listings,
        )

        create_table_dict[task] = create_table_task

    # load or update data to postgres database
    load_task_dict = {}
    task_args = ['host_info', 'hotel_location', 'hotel_facilities', 'price_info', 'host_metrics']
    for task_name in task_args:
        # Define a PythonOperator that uses the above function as its task
        task_id = task_name
        # print(task_name)
        task = PythonOperator(
            task_id = task_id,
            python_callable=load_list_postgres,
            # op_args={'table_name': task_id}, 
            op_args=[task_name], 
            #provide_context=True,  # This is needed to pass the context to the PythonOperator
            dag=dag_listings,
        )

        load_task_dict[task_id] = task

    # load or update data to mongodb database
    #mongodata_name = 'hotel&host_text'
    load_lists_mongo_task = PythonOperator(
        task_id='load_lists_mongo',
        python_callable=load_lists_mongo,
        #op_args={'Mongodata_name': mongodata_name},
        #provide_context=True,  # This is needed to pass the context to the PythonOperator
        dag=dag_listings,
    )


## dependencies of tasks ## 
# create tables
start_task >> [create_table_dict[task_name] for task_name in create_table_dict]
# process data
start_task >> extract_lists_task >> Proccess_lists
# load data to Postgresql
Proccess_lists >> [load_task_dict[task_name] for task_name in task_args]
# load data to MongoDB
Proccess_lists >> load_lists_mongo_task