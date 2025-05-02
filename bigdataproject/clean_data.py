from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data
from datetime import datetime, date, time
import json

def add_geopoint( collection ):
    """
        Simple method here: Some documents have a field called location, which contains
        a point for longitude and a point for latitude. I want to use those
        to make a single geopoint that can be plotted on a map in Atlas
    """

    for document in collection.find( {"location" : {"$exists": True}}):
        try:
            latitude = float( document.get( "latitude" ) )
            longitude = float( document.get( "longitude" ) )
            collection.update_one(
                {"_id" : document["_id"]},
                {
                    "$set" : {"geopoint" : {"type" : "Point", "coordinates" : [longitude, latitude]}},
                    "$unset" : {"location": ""}
                }
            )
        except ( TypeError, ValueError ):
            print( f"Skipping document with _id: {document['_id']} due to invalid lat/lon." )


def clean_datetimes( collection ):
    """
        All of the date and time information is in the form of 2 fields: date and time
        These can be combined into one field with date and time in it
    """
    for document in collection.find( {"crash_time" : {"$exists": True}} ):
        try:
            
            crash_date = document.get( "crash_date" )
            crash_time = document.get( "crash_time" )

            if crash_date and crash_time:
                
                crash_date_date = datetime.strptime(crash_date[:10].strip(), "%Y-%m-%d").date()
                crash_date_time = datetime.strptime( crash_time.strip(), "%H:%M" ).time()
                combined_datetime = datetime.combine( crash_date_date, crash_date_time )

                collection.update_one(
                {"_id" : document["_id"]},
                {
                    "$set" : {"crash_date" : combined_datetime},
                    "$unset" : {"crash_time" : ""}
                }
            )
                
        except Exception as e:
            print( f"Error processing document {document['_id']}: {e}" )



def main():
    crashes = return_collection( connect_to_database(), "crashes" )

    clean_datetimes( crashes )


main()



