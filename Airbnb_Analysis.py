# MongoDB and PostgreSQL
from pymongo import MongoClient
from sqlalchemy import create_engine

# Other Modules
import pandas as pd


def make_connection():
    # Connect to MongoDB
    client = MongoClient('mongodb://mongo:mongo@localhost:27017/airbnb')
    # 'mongodb://username:password@localhost:27017/your_database'
    # Access a specific database and collection
    db = client['airbnb']
    collection = db['airbnb_hotel_info']

    # Connect to PostgreSQL
    postgres_url = "postgresql+psycopg2://airflow:airflow@172.18.0.4:5432/airflow"
    conn = create_engine(postgres_url)
    # Create a cursor

    return client, conn

def data_analysis1(collection, conn):

    ############## get address and price of listings where the place is described as "quiet" ##############
    ### MongoDB Part ###
    # Define the regex pattern for the word 'quiet'
    regex_pattern = 'quiet'
    # Create the query using $regex
    query = {
        '$or': [
            {'summary': {'$regex': regex_pattern, '$options': 'i'}},  # 'i' for case-insensitive
            {'space': {'$regex': regex_pattern, '$options': 'i'}},
            {'description': {'$regex': regex_pattern, '$options': 'i'}},
            {'reviews.comments': {'$regex': regex_pattern, '$options': 'i'}}
        ]
    }
    projection = {'id': 1}
    # Execute the query
    result = collection.find(query,projection)
    id_list = []
    for doc in result:
        id_list.append(doc['id'])

    # Convert each element to a string
    string_list = map(str, id_list)

    # Join the string elements with commas
    result_string = ",".join(string_list)

    # Surround the result with parentheses
    id_list = f"({result_string})"

    ### PostgreSQL Part ###
    # Construct the SQL query with placeholders for the IDs
    query = """SELECT h.id as id, 
                      CONCAT(neighborhood, street, ',', CAST(zipcode AS VARCHAR)) AS address,
                      price as price_per_night
                FROM hotel_location as h, price_info as p 
                WHERE h.id = p.id and
                h.id IN""" + id_list +";"
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('/opt/airflow/dags/Airbnb_Analysis/data/data_analysis1.csv', index=False)

def data_analysis2(conn):

    ### PostgreSQL Part ###
    # Construct the SQL query with placeholders for the IDs
    query = """SELECT hl.id as id, 
                      CONCAT(neighborhood, street, ',', CAST(zipcode AS VARCHAR)) AS address,
                      weekly_price as weekly_price
                FROM hotel_location as hl
                Join hotel_facilities as hf ON hl.id = hf.id
                Join price_info as p ON hl.id = p.id
                WHERE hl.City = 'Washington' and
                      hf.bedrooms = 1 and
                      hf.property_type = 'Apartment'; """
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('/opt/airflow/dags/Airbnb_Analysis/data/data_analysis2.csv', index=False)

def data_analysis3(conn):
    ### PostgreSQL Part ###
    # Construct the SQL query with placeholders for the IDs
    query = """SELECT city, 
                      count(property_type) as bed_breakfast,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY replace(substring(price from 2),',','')::real) as median_price
                FROM hotel_location as hl
                Join hotel_facilities as hf ON hl.id = hf.id
                Join price_info as p ON hl.id = p.id
                WHERE property_type = 'Bed & Breakfast'
                GROUP BY city; """
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('/opt/airflow/dags/Airbnb_Analysis/data/data_analysis3.csv', index=False)

def data_analysis4(conn):
    ### PostgreSQL Part ###
    # Construct the SQL query with placeholders for the IDs
    query = """select h1.City from
                    (select City,
                            AVG(replace(substring(price FROM 2),',','')::real) as avg1
                            FROM hotel_location as hl
                            Join hotel_facilities as hf ON hl.id = hf.id
                            Join price_info as p ON hl.id = p.id
                            where property_type = 'House'
                            group by City) as h1
                    inner join
                    (select City,
                            AVG(replace(substring(price FROM 2),',','')::real) as avg2
                            FROM hotel_location as hl
                            Join hotel_facilities as hf ON hl.id = hf.id
                            Join price_info as p ON hl.id = p.id
                            where property_type = 'Townhouse'
                            group by City) as h2
                on h1.City = h2.City
                where h1.avg1 < h2.avg2;"""
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('/opt/airflow/dags/Airbnb_Analysis/data/data_analysis4.csv', index=False)

def data_analysis5(collection, conn):

    ############## how many listings per city mention parks and museums ##############
    ### MongoDB Part ###
    # Define the regex pattern for the word 'quiet'
    # Create the query using $regex
    query = {
        '$and': [
            {
            '$or': [
                {'summary': {'$regex': 'park', '$options': 'i'}},
                {'space': {'$regex': 'park', '$options': 'i'}},
                {'description': {'$regex': 'park', '$options': 'i'}},
                {'neighborhood_overview': {'$regex': 'park', '$options': 'i'}},
                {'notes': {'$regex': 'park', '$options': 'i'}}
                #{'amenities': 'park'}  # Assuming 'amenities' is an array
            ]
            },
            {
            '$or': [
                {'summary': {'$regex': 'museum', '$options': 'i'}},
                {'space': {'$regex': 'museum', '$options': 'i'}},
                {'description': {'$regex': 'museum', '$options': 'i'}},
                {'neighborhood_overview': {'$regex': 'museum', '$options': 'i'}},
                {'notes': {'$regex': 'museum', '$options': 'i'}}
                #{'amenities': 'museum'}  # Assuming 'amenities' is an array
            ]
            }
        ]
    }

    projection = {'id': 1}
    # Execute the query
    result = collection.find(query,projection)

    id_list = []
    for doc in result:
        id_list.append(doc['id'])

    # Convert each element to a string
    string_list = map(str, id_list)

    # Join the string elements with commas
    result_string = ",".join(string_list)

    # Surround the result with parentheses
    id_list = f"({result_string})"

    ### PostgreSQL Part ###
    # Construct the SQL query with placeholders for the IDs
    query = """SELECT count(*) as number_of_listings
               FROM hotel_facilities as hf
               JOIN hotel_location as hl
               ON hf.id = hl.id
               WHERE 'park' = any(amenities) and
                     'museum' = any(amenities) and
               hl.id IN""" + id_list + "group by city;"
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('/opt/airflow/dags/Airbnb_Analysis/data/data_analysis5.csv', index=False)

def data_analysis6(collection):

    ############## get address and price of listings where the place is described as "quiet" ##############
    ### MongoDB Part ###
    pipeline = [
        {
            '$match': {
                'reviews.comments': {'$regex': 'automated posting', '$options': 'i'}  # Add condition for comments
            }
        },
        {
            '$project': {
                'reviews.id': 1,  # Include the 'id' field under the 'reviews' field
                'reviews.date': 1,  # Include the 'date' field under the 'reviews' field
                'reviews.reviewer_id': 1,  # Include the 'reviewer_id' field under the 'reviews' field
                'reviews.reviewer_name': 1,  # Include the 'reviewer_name' field under the 'reviews' field
                'cancel_days': {
                    '$cond': {
                        'if': {'$eq': [{'$regexMatch': {'input': '$reviews.comments', 'regex': '\\d+'}}, None]},
                        'then': 1,
                        'else': {'$regexMatch': {'input': '$reviews.comments', 'regex': '\\d+'}}
                    }
                }
            }
        }
    ]

    # execute the query
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)

    # delete '_id' column
    df.drop(columns=['_id'], inplace=True)

    # save dataframe as CSV file
    df.to_csv('output.csv', index=False)

if __name__ == "__main__":
    collection, conn = make_connection()
    data_analysis1(collection, conn)
    data_analysis2(conn) 
    data_analysis3(conn)
    data_analysis4(conn)
    data_analysis5(collection, conn) 
    data_analysis6(collection)
