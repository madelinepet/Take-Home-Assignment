# Please develop your ingestion service in Python. You may select the delivery format (e.g., Jupyter
# Notebook, containerized microservice). For this exercise, you may assume that a scheduling service
# to regularly invoke your ingestion is provided.
# Where and how you process the data is at your discretion.

import os
import requests
# import psycopg2
import pandas as pd
import geopandas as gpd
from zipfile import ZipFile
from shapely.geometry import Point
from urllib.request import urlretrieve
from requests.exceptions import RequestException
from zipfile import BadZipFile
from psycopg2 import OperationalError
from mappings import event_root_codes, event_base_codes, event_codes, map_fips_to_iso2

def main():
    """ Main controller function
    """
    try:
        extracted_file_path, zip_file_path = retreive_event_data()
        geo_data = retreive_geo_data()
        cleaned_data = clean_data(extracted_file_path)
        filtered_event_data = filter_data(cleaned_data, geo_data)
        load_db(filtered_event_data, event_root_codes, event_base_codes, event_codes, map_fips_to_iso2)
    except RequestException as e:
        print(f"Error while retrieving data: {e}")
    except BadZipFile as e:
        print(f"Error while extracting the zip file: {e}")
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        cleanup(extracted_file_path, zip_file_path)


def retreive_event_data() -> str:
    """ Gets event data from external source.
        I would improve this by looking into the GDELT API.
    """
    # Retrieve data from the source site
    data_files = requests.get('http://data.gdeltproject.org/gdeltv2/lastupdate.txt').content.decode()

    # Selecting the first entry with “export” in it will
    # give you the latest 15 min worth of data
    file_download_location = data_files.replace("\n", " ").split(" ")[2]

    # get just the file name out of the url
    file_name = file_download_location.split("/")[-1]
    file_path = 'files/'+ file_name

    # downloading the file to files/
    urlretrieve(file_download_location, file_path)

    # unzip and extract file to extracted/
    with ZipFile(file_path, 'r') as zip:
        zip.extractall('extracted/')

    # remove .zip suffix
    extracted_file_path = 'extracted/' + file_name[0:-4]

    print('File downloaded')
    return extracted_file_path, file_path

def clean_data(extracted_file_path):
    """ Perform some foundational data prep and quality assurance
    """
    try:
        # load event data into pandas df
        event_df = pd.read_csv(extracted_file_path, sep='\t')

        # name cols so df is easier to use
        event_df.columns = ['col_' + str(i) for i  in range(61)]

        # there are many things I could do here if I had more time
        # for now I will drop duplicates and remove columns that aren't needed
        # To make this more robust, I would clean and standardize the text
        # convert dates and floats to the appropriate formats/types
        # I would also do ifnull checks and add in logic to fill in null values as needed

        # Select cols needed in final output defined in assignment
        event_df = event_df[['col_0','col_1', 'col_26','col_27', 'col_28','col_52', 'col_53','col_56', 'col_57','col_59', 'col_60']]
        
        # name the columns according to doc
        event_df.columns = ['GLOBALEVENTID', 'SQLDATE', 'EventCode', 'EventBaseCode', 'EventRootCode', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_Lat', 'ActionGeo_Long', 'DATEADDED', 'SOURCEURL']
        # Drop duplicates
        event_df = event_df.drop_duplicates()

        return event_df
    
    except pd.errors.EmptyDataError as e:
        raise pd.errors.EmptyDataError(f"Empty data error: {e}")
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Parser error: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred during data cleaning: {e}") 

def retreive_geo_data():
    """ In addition to the above source data, geometric location data for US counties may be located at:
        https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json
    """
    print('Retreiving geo data')
    return requests.get('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json').content.decode()

def filter_data(event_df, geo_data):
    """ Please filter the event data to those events located within the US 
        based on their lat/lon coordinates (lat: col 56, long:col 57)
    """
    # Load choropleth data using geopandas
    choropleth_df = gpd.read_file(geo_data)

    # Convert the event dataframe to a GeoDataFrame using "ActionGeo_Lat" and "ActionGeo_Long" columns
    # which are lat and long respectively
    event_df['geometry'] = event_df.apply(lambda row: Point(row['ActionGeo_Long'], row['ActionGeo_Lat']), axis=1)
    # Specify the CRS for the event data
    event_gdf = gpd.GeoDataFrame(event_df, geometry='geometry', crs="EPSG:4326")

    # Ensure that both datasets have the same CRS
    event_gdf = event_gdf.to_crs(choropleth_df.crs)

    # Perform the spatial join to filter events in the U.S.
    us_events = gpd.sjoin(event_gdf, choropleth_df, how='inner', predicate='intersects')

    print('Data filtered - might add in specifics using variables here')
    return us_events

def load_db(filtered_event_data, event_root_codes, event_base_codes, event_codes, map_fips_to_iso2):
    """ Please use Postgres/GIS as your target database. You should demonstrate how you might make
        and manage the database connection, as well as the execution of needed transactions. You do not
        need to configure and run the actual database except as it is helpful to you to do so.
    """
    # This is just example code

    # # Define the database connection parameters
    # database_uri = "dbname=mydatabase user=myuser password=mypassword host=localhost"

    # # Establish a connection to the database
    # connection = psycopg2.connect(database_uri)

    # # Create a cursor for executing SQL commands
    # cursor = connection.cursor()

    # create_table_sql = """
    # CREATE TABLE events (
    #     GLOBALEVENTID SERIAL PRIMARY KEY,
    #     SQLDATE DATE,
    #     EventCode VARCHAR,
    #     EventBaseCode VARCHAR,
    #     EventRootCode VARCHAR,
    #     ActionGeo_FullName VARCHAR,
    #     ActionGeo_CountryCode VARCHAR,
    #     ActionGeo_Lat FLOAT,
    #     ActionGeo_Long FLOAT,
    #     DATEADDED DATE,
    #     SOURCEURL TEXT
    # )
    # """

    # I would also add the JSON mappings into the database as dimension tables
    # By creating the tables and inserting the given values into them

    # # Execute the SQL command to create the table
    # cursor.execute(create_table_sql)
    # connection.commit()

    # us_events.to_sql("events", connection, if_exists="replace", index=False)
    # connection.commit()


    # cursor.close()
    # connection.close()
    
    print('DB fictionally loaded: fictionally variable number of rows inserted')

def cleanup(extracted_file_path, zip_file_path):
    """ Removes downloaded and extracted files at end
    """
    print('Removing files')
    os.remove(extracted_file_path)
    os.remove(zip_file_path)


if __name__ == "__main__": 
    main()