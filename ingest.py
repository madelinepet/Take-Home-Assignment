# Please develop your ingestion service in Python. You may select the delivery format (e.g., Jupyter
# Notebook, containerized microservice). For this exercise, you may assume that a scheduling service
# to regularly invoke your ingestion is provided.
# Where and how you process the data is at your discretion.
# Please use Postgres/GIS as your target database. You should demonstrate how you might make
# and manage the database connection, as well as the execution of needed transactions. You do not
# need to configure and run the actual database except as it is helpful to you to do so.

from mappings import event_root_codes, event_base_codes, event_codes
#map_fips_to_iso2
from urllib.request import urlretrieve
import requests
from zipfile import ZipFile


# Retrieve data from the source site
data_files = requests.get('http://data.gdeltproject.org/gdeltv2/lastupdate.txt').content.decode()
# Selecting the first entry with “export” in it will
# give you the latest 15 min worth of data
file_download_location = data_files.replace("\n", " ").split(" ")[2]
file_name = 'files/' + file_download_location.split("/")[-1]
# downloading the file to files/
last_15_file = urlretrieve(file_download_location, file_name)
# unzip and read file
with ZipFile(file_name, 'r') as zip:
    zip.extractall('extracted/')

# In addition to the above source data, geometric location data for US counties may be located at:
# https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json
geo_data = requests.get('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json').content.decode()
print(geo_data)
# Please filter the event data to those events located within the US 
# based on their lat/lon coordinates


# TODO:
# might need this
# data=pandas.read_csv('filename.tsv',sep='\t')


# Perform some foundational data prep and quality assurance

# Load the data into a database

# Dockerize
# Freeze into req's 
# make quotes consistent
# write comments about using api
# replace file download logic with better logic
# gitignore
# delete file when done
# create functions
# tests

# if __name__ == "__main__": 
#     main()