import pandas as pd
import psycopg2
import sys

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
                df = pd.DataFrame(data)
                df['hotel_country'] =  df['hotel_country'].astype('category')
                df['hotel_category'] =  df['hotel_category'].astype('category')
                df['departure_city'] =  df['departure_city'].astype('category')
                df['tour_operator_name'] =  df['tour_operator_name'].astype('category')
                df['rating_value'] =  df['rating_value'].astype('category')
                insert_data = df.values.tolist()
                cur.executemany(insert_query, insert_data)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()