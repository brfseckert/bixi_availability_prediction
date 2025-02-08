""""
Module to store constants used throughout the project
"""

BIXI_OPEN_DATA_URL = 'https://bixi.com/en/open-data/'
STATION_CAPACITY_URL= 'https://gbfs.velobixi.com/gbfs/en/station_information.json'
RAW_DATA_DIRECTORY = '/data'
COLUMN_MAPPING = {
    'pk':'code',
    'emplacement_pk_start': 'code_start_station',
    'emplacement_pk_end': 'code_end_station',
    'startstationname': 'name_start_station', 
    'startstationarrondissement':'arrondissement_start_station',
    'startstationlatitude':'latitude_start_station', 
    'startstationlongitude':'longitude_start_station',
    'endstationname': 'name_end_station',
    'endstationarrondissement':'arrondissement_end_station', 
    'endstationlatitude':'latitude_end_station', 
    'endstationlongitude':'longitude_end_station',
    'starttimems':'start_date', 
    'endtimems': 'end_date',
    'start_station_code':'code_start_station',
    'end_station_code':'code_end_station'
}