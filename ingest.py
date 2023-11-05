# Please develop your ingestion service in Python. You may select the delivery format (e.g., Jupyter
# Notebook, containerized microservice). For this exercise, you may assume that a scheduling service
# to regularly invoke your ingestion is provided.
# Where and how you process the data is at your discretion.

from mappings import event_root_codes, event_base_codes, event_codes, map_fips_to_iso2
from urllib.request import urlretrieve
import requests
from zipfile import ZipFile
import pandas as pd
import os


def main():
    """ Main controller function
    """
    extracted_file_path, zip_file_path = retreive_event_data()
    filtered_event_data = filter_data(extracted_file_path)
    geo_data = retreive_geo_data()
    load_db(filtered_event_data, geo_data, event_root_codes, event_base_codes, event_codes, map_fips_to_iso2)
    cleanup(extracted_file_path, zip_file_path)


def retreive_event_data() -> str:
    """ Gets data from external sources
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
    # unzip and extract file
    with ZipFile(file_path, 'r') as zip:
        zip.extractall('extracted/')
    # remove .zip suffix
    extracted_file_path = 'extracted/' + file_name[0:-4]
    print('File downloaded')
    return extracted_file_path, file_path

def clean_data():
    """
    """
    pass

def filter_data(extracted_file_path):
    """
    """
    # load event data into pandas df
    df = pd.read_csv(extracted_file_path, sep='\t')

    # name cols so df is easier to use
    df.columns = ['col_' + str(i) for i  in range(61)]

    # Please filter the event data to those events located within the US 
    # based on their lat/lon coordinates (lat: col 56, long:col 57)
    # source for equation: https://latitudelongitude.org/us/
    df_filtered = df.query('col_56 >= 19.50139 & col_56 <= 64.85694 & col_57 >= -161.75583 & col_57 <= -68.01197')
    print('Filtering data')
    return df_filtered

def retreive_geo_data():
    """ In addition to the above source data, geometric location data for US counties may be located at:
        https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json
    """
    print('Retreiving geo data')
    return requests.get('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json').content.decode()

def load_db(filtered_event_data, geo_data, event_root_codes, event_base_codes, event_codes, map_fips_to_iso2):
    """ Please use Postgres/GIS as your target database. You should demonstrate how you might make
        and manage the database connection, as well as the execution of needed transactions. You do not
        need to configure and run the actual database except as it is helpful to you to do so.
    """
    # format table with names as suggested in doc
    print('Loading db')

def cleanup(extracted_file_path, zip_file_path):
    """ Removes downloaded and extracted files at end
    """
    print('Removing files')
    os.remove(extracted_file_path)
    os.remove(zip_file_path)
    

    # TODO:

    # Perform some foundational data prep and quality assurance

    # Load the data into a database

    # Dockerize
    # Freeze into req's 
    # make quotes consistent
    # write comments about using api
    # replace file download logic with better logic - or at least discuss making better with API etc
    # gitignore
    # delete file when done programatically
    # create functions
    # tests
    # talk about branching correctly
    # talk about star schema with those objs as dims
    # write about imagining print statements as logging
    # write about running script
    # db connection
    # filter data down to only specified cols

if __name__ == "__main__": 
    main()