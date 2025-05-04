from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data, make_collection
from datetime import datetime, date, time
import json

def add_geopoint( find_collection, change_collection=None ):
    """
        Simple method here: Some documents have a field called location, which contains
        a point for longitude and a point for latitude. I want to use those
        to make a single geopoint that can be plotted on a map in Atlas. There's an optional
        field if you want to have the changes be made in a new collection
        instead of the original one for some reason
    """

    #If a 2nd parameter is specified, we need to check ids there
    if change_collection is not None:
        unique_ids = set(doc["_id"] for doc in change_collection.find({}, {"_id": 1}))

    last_id = None

    #I was having an issue with cursor timeout and my free tier of Atlas does not allow me
    #to disable timeout, so I had to use batches of 1000
    while True:
        query = {"location": {"$exists": True}}
        if last_id:
            query["_id"] = {"$gt": last_id}

        batch = list(find_collection.find(query).sort("_id", 1).limit(1000))

        if not batch:
            break

        #for every doc we look at, try to get lat and long. They won't all have it, so it's in a try
        #If we want to chuck it into a new collection, copy the doc, make the geopoint, and toss it in.
        #also remove the location field since that's redundant and not a geopoint object
        for document in batch:
            last_id = document["_id"]   
            try: 
                latitude = float( document.get( "latitude" ) )
                longitude = float( document.get( "longitude" ) )

                if change_collection is not None:

                    new_document = document.copy()
                    new_document["geopoint"] = {
                        "type": "Point",
                        "coordinates": [longitude, latitude]
                    }
                    new_document.pop( "location", None )
                    
                    if new_document["_id"] not in unique_ids:
                            unique_ids.add( new_document["_id"] )
                            change_collection.insert_one( new_document )

                #if we are making the changes in the main collection, just update and set the new field and unset location
                else:

                    find_collection.update_one(
                        {"_id" : document["_id"]},
                        {
                            "$set" : {"geopoint" : {"type" : "Point", "coordinates" : [longitude, latitude]}},
                            "$unset" : {"location": ""}
                        }
                    )

            except Exception:
                print( f"Skipping document with _id: {document['_id']} due to invalid lat/lon." )


def clean_datetimes( find_collection, change_collection=None ):
    """
        All of the date and time information is in the form of 2 fields: date and time
        These can be combined into one field with date and time in it. There's an optional
        field if you want to have the changes be made in a new collection
        instead of the original one for some reason
    """

    #Same thing as geopoint for looking at ids in new collection if we specify
    if change_collection is not None:
        unique_ids = set(doc["_id"] for doc in change_collection.find({}, {"_id": 1}))

    last_id = None

    #same thing as geopoint for having to do batches because Atlas and MongoDB hate their free developers
    while True:
        query = {"crash_date": {"$exists": True}}
        if last_id:
            query["_id"] = {"$gt": last_id}

        batch = list(find_collection.find(query).sort("_id", 1).limit(1000))

        if not batch:
            break

        for document in batch:

            try:
                last_id = document["_id"]

                crash_date = document.get( "crash_date" )
                crash_time = document.get( "crash_time" )

                if crash_date and crash_time:
                    
                    #super cool of NYC to make the date of the crash and time different fields
                    #even cooler that they're both strings even though crash_date is formatted to look like a datetime
                    crash_date_date = datetime.strptime(crash_date[:10].strip(), "%Y-%m-%d").date()
                    crash_date_time = datetime.strptime( crash_time.strip(), "%H:%M" ).time()
                    combined_datetime = datetime.combine( crash_date_date, crash_date_time )

                    if change_collection is not None:
                        
                        try: 
                            new_document = document.copy()
                            new_document["crash_date"] = { "crash_date" : combined_datetime }

                            if new_document["_id"] not in unique_ids:
                                unique_ids.add( new_document["_id"] )
                                change_collection.insert_one( new_document )

                        except Exception as e:
                            print( f"Error processing document {document['_id']}: {e}" )
                    
                    else:

                        find_collection.update_one(
                            {"_id" : document["_id"]},
                            {
                                "$set" : {"crash_date" : combined_datetime},
                                "$unset" : {"crash_time" : ""}
                            }
                        )
                
            except Exception as e:
                print( f"Error processing document {document['_id']}: {e}" )


def remove_duplicates(collection):
    """
    Note: Generative A.I. was used for this method because I was unsure about how to do it
    The code provided was bad and overcomplicated, so I removed anything I felt was unnecessary


    Removes duplicate documents from a MongoDB collection by comparing the
    contents of each document (excluding '_id'). Keeps the first occurrence
    and removes all others.
    """
    seen_ids = set()
    duplicates = []

    for doc in collection.find():
        
        doc_id = document.get( "_id" )

        if doc_id in seen_ids:
            duplicates.append(doc['_id'])
        else:
            seen_ids.add(doc_id)

    if duplicates:
        result = collection.delete_many({'_id': {'$in': duplicates}})
        print(f"Removed {result.deleted_count} duplicate documents.")
    else:
        print("No duplicates found.")

def main():
    
    database = connect_to_database()
    crashes = return_collection( database, "crashes" )

    #make_collection( database, "cleaned_crashes" )
    cleaned_crashes = return_collection( database, "cleaned_crashes" )

    #add_geopoint( crashes, cleaned_crashes )
    clean_datetimes( cleaned_crashes )


main()