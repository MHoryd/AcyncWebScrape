import yaml
import os

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
    countries_list = config['Countries']
    tour_operators = config['Tour_Operators']
    db_user = config['db_user']
    db_password = config['db_password']
    database = config['database']
    port = config['port']
    host = config['host']
    batch_size = config['Batch_size']
    csvfile = config['csvfile']
    