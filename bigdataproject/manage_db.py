from db_config import get_mongodb_connection
import requests
import json



def connect_to_database():
    """
        This will just connect to the database configured in db_config.py
    """

    try:
        client = get_mongodb_connection()
        print( "Successfully connected to MongoDB" )
        return client

    except Exception as e:
        print( f"Could not connect to MongoDB: {e}" )
        return None


def return_collection( database, collection_name ):
    """
        Given a collection name in db_config.py, this will connect to that collection
    """

    try:
        collection = database.get_collection( collection_name )
        print( f"Successfully connected to connection: {collection_name}" )
        return collection

    except Exception as e:
        print( f"Could not connect to collection: {collection_name}. See reason: {e}" )


def populate_database( collection_name, input ):
    """
        This will take a json input and throw it into the specified collection.
        We are assuming the json is a string, like the output of get_data
    """
    try: 
        collection_name.insert_many( input )
        print( f"Successfully inserted data into collection: {collection_name}" )
    
    except Exception as e:
        print( f"Failed to insert into collection for reason: {e}" )


def get_data( site_string, number_of_rows=100000, pagination=1000, start=0 ):
    """
        This will take a site string, a number of rows (default of 100k) and pagination
        (default of 1k). I'm assuming the data in question does not require a key because
        I did not want to make a free account for some site that I'd only use once for this
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BigDataProject/1.0)"
    }


    json_string = ''
    for offset in range( start, number_of_rows + start, pagination ):
        response = requests.get( site_string + '?$offset=' + str( offset ), headers = headers )
        if offset > start:
            json_string = json_string + ', ' + json.dumps( response.json() )[1:-1]
        else:
            json_string = json.dumps( response.json() )[:-1]
    
    json_string = json_string + ']'
    return json.loads( json_string )


def make_collection( database, collection_name ):
    """
        Just making a collection. Nothing too difficult
    """
    try: 
        database.create_collection( collection_name )

    except Exception as e:
        print( f"Collection: {collection_name} already exists" )

