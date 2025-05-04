from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data, make_collection
from datetime import datetime, date, time
import json
import hashlib

def add_geopoint( find_collection, change_collection=None ):
    """
        Simple method here: Some documents have a field called location, which contains
        a point for longitude and a point for latitude. I want to use those
        to make a single geopoint that can be plotted on a map in Atlas. There's an optional
        field if you want to have the changes be made in a new collection
        instead of the original one for some reason
    """
    if change_collection is not None:
        unique_ids = set(doc["_id"] for doc in change_collection.find({}, {"_id": 1}))

    last_id = None

    while True:
        query = {"location": {"$exists": True}}
        if last_id:
            query["_id"] = {"$gt": last_id}

        batch = list(find_collection.find(query).sort("_id", 1).limit(1000))

        if not batch:
            break

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
    if change_collection is not None:
        unique_ids = set(doc["_id"] for doc in change_collection.find({}, {"_id": 1}))

    last_id = None

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


    Removes duplicate documents from a MongoDB collection by comparing the
    contents of each document (excluding '_id'). Keeps the first occurrence
    and removes all others.
    """
    seen_hashes = set()
    duplicates = []

    for doc in collection.find():
        doc_copy = {k: v for k, v in doc.items() if k != '_id'}  # Exclude _id
        doc_str = json.dumps(doc_copy, sort_keys=True)           # Stable string
        doc_hash = hashlib.md5(doc_str.encode()).hexdigest()     # Hash document

        if doc_hash in seen_hashes:
            duplicates.append(doc['_id'])  # Mark duplicate for deletion
        else:
            seen_hashes.add(doc_hash)

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