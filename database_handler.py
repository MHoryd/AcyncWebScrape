import pandas as pd
import psycopg2
from asynctinydb import TinyDB
from tinydb import TinyDB as base_tinydb

def insert_data(data,config):
    conn = None
    try:
        with  psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.db_user,
            password=config.db_password
            ) as conn:

            with conn.cursor() as cur:


                insert_query = """
                    INSERT INTO scraped_data.raw (
                        hotel_name, hotel_country, hotel_region, hotel_city,
                        hotel_category, stay_duration, price, departure_city,
                        departure_date, return_date, tour_operator_name, rating_value,created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(insert_query, data)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


async def insert_data_to_tinydb(data):
    db = TinyDB('db.json')
    await db.insert_multiple(data)

def get_data_from_tinydb():
    db = base_tinydb('db.json')
    documents =  db.all()
    return [dict(doc) for doc in documents] 
    
def clear_tinydb():
    db = base_tinydb('test.json')
    db.truncate()

def convert_to_list_of_values(data):
    return [list(values.values()) for values in data]