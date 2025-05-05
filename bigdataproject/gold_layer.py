from db_config import get_mongodb_connection
from manage_db import return_collection, connect_to_database, populate_database, get_data, make_collection
import datetime
import folium
from folium.plugins import HeatMap
from collections import defaultdict


def make_heatmap( collection_name, coord_level=3, time=False ):
    """
        Making a heatmap for the density of _ids by long/lat in the data
        I did use external sources for help. Can be found here:
            https://medium.com/@vinodvidhole/interesting-heatmaps-using-python-folium-ee41b118a996
        Did not know how to extract a count of _id for each lat/long, so I iterated through all docs 
        Does not work with time heatmap, but that's not my problem right now. I am just using a regular one
    """
    
    documents = collection_name.find(
        {
            "latitude": {"$exists": True},
            "longitude": {"$exists": True}
        }
    )

    
    heatmap_density = defaultdict( int )
    for document in documents:
        
        latitude = round( float( document["latitude"] ), coord_level )
        longitude = round( float( document["longitude"] ), coord_level )
        
        #we only care about dates if the time heatmap option is selected
        if time:
            date = datetime.date()
            heatmap_density[( latitude, longitude, date )] += 1
        else:
            heatmap_density[( latitude, longitude )] += 1

    #according to the tutorial, I need a list of all the times, hence the time_index, which is at a date level
    if time:
        heatmap_data = [[latitude, longitude, date, count] for ( latitude, longitude, date ), count in heatmap_density.items()]
        time_index = [entry["date"] for entry in heatmap_data if "date" in entry]
    else:
        heatmap_data = [[latitude, longitude, count] for ( latitude, longitude ), count in heatmap_density.items()]


    #Making a center for the map to default to so it knows what to show when I open it
    if heatmap_data:
        center = [sum( point[0] for point in heatmap_data ) / len( heatmap_data ),
                sum( point[1] for point in heatmap_data ) / len( heatmap_data )]
    else:
        center = [0, 0]


    actual_heatmap = folium.Map( location=center, zoom_start=10, tiles="Cartodb Positron" )

    if time:
        HeatMapWithTime( heatmap_data, radius = 10, index = time_index, auto_play = True, use_local_extrema=True, max_zoom=13 ).add_to( actual_heatmap )
        actual_heatmap.save( "weighted_time_heatmap.html" )
    else:
        HeatMap( heatmap_data, radius = 10, max_zoom=13 ).add_to( actual_heatmap )
        actual_heatmap.save( "weighted_heatmap.html" )


def main():
    
    
    database = connect_to_database()
    collection = return_collection( database, "cleaned_crashes" )

    #make_heatmap( cleaned_crashes, 6, False )




main()