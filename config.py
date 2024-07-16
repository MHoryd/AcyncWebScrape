import yaml


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
    mongo_db_user = config['Mongo_db_user']
    mongo_db_password = config['Mongo_db_password']
    mongo_database = config['Mongo_database']
    mongo_collection = config['Mongo_collection']
    mongo_port = config['Mongo_port']