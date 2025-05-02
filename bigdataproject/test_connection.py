from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data, make_collection
import requests
import json

def main():
    
    database = connect_to_database()
    make_collection( database, "crashes" )
    make_collection( database, "cleaned_crashes" )

    crashes = return_collection( database, "crashes" )
    #get_data( 'https://data.cityofnewyork.us/resource/h9gi-nx95.json', 10000, 1000, 60000 )

    populate_database( crashes, get_data( 'https://data.cityofnewyork.us/resource/h9gi-nx95.json', 100000, 1000, 0 ) )

main()
