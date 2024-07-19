import psycopg2
import csv
import os
import pandas as pd
from sqlalchemy import create_engine

def insert_data(df,config):
    try:    
        connection_url = f'postgresql+psycopg2://{config.db_user}:{config.db_password}@{config.host}:{config.port}/{config.database}'
        engine = create_engine(connection_url)
        with engine.connect() as connection:
            df.to_sql(con=connection,schema='scraped_data',name='raw', if_exists='replace')
    except Exception as e:
            print(e)

def dump_data_to_csv(data, filename):

    file_exists = os.path.isfile(filename)
    headers = ['hotel_name','hotel_country','hotel_region','hotel_city','hotel_category','stay_duration','price','departure_city','departure_date','return_date','tour_operator_name','rating_value','created_at']
    
    with open(filename, 'a', newline='', encoding='utf=8') as csvfile:
        writer = csv.writer(csvfile)
        
        if not file_exists:
            writer.writerow(headers)
        
        for row in data:
            writer.writerow(row)


def get_data_from_csv(filename):
    df = pd.read_csv(filename)
    df['hotel_country'] =  df['hotel_country'].astype('category')
    df['hotel_category'] =  df['hotel_category'].astype('category')
    df['departure_city'] =  df['departure_city'].astype('category')
    df['tour_operator_name'] =  df['tour_operator_name'].astype('category')
    df['rating_value'] =  df['rating_value'].astype('category')
    return df

def delete_temp_csv(filename):
    os.remove(filename)