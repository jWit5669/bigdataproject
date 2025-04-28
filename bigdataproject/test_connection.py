from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data
import requests
import json

def main():
    
    crashes = return_collection( connect_to_database(), "crashes" )

    populate_database( crashes, get_data( 'https://data.cityofnewyork.us/resource/h9gi-nx95.json', 100000, 1000 ) )

main()
