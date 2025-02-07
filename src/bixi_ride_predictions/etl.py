"""
Module dedicated to etl operations related to the data sources of the project
"""

import requests
from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv, concat
from typing import List, Dict
from re import search
import os
import logging
import zipfile

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class RidesDataPipeline:
    """
    Extract, load and transform pipeline object for bixi rides data
    """

    def __init__(self, url: str, temporary_directory: str, years: Tuple[int, int], column_name_mapping: Dict[str, str], ride_data_columns: List[str]):
        self.url = url
        self.raw_data_directory = temporary_directory
        self.years = [y for y in range(years[0], years[1] +1)]
        self.column_name_mapping = column_name_mapping
        self.ride_data_columns = ride_data_columns

    def _get_s3_endpoints(self) -> Dict[int, str]:
        """Webscrape and extract all available s3 endpoints metadata from bixi's website"""
        
        response = requests.get(self.url, 'html.parser')
        soup_classes = (
            BeautifulSoup(response.content)
            .find_all('a', href=True, attrs={'class':'button button-primary'})
        )
        s3_endpoints = [s3['href'] for s3 in soup_classes if 's3.ca' in s3['href']]

        s3_endpoint_by_year = {
            int(search('2[0-9]{3}', endpoint.split('/')[-1])[0]) : endpoint # Get only endpoints with year in the string
            for endpoint in s3_endpoints
        }
         
        return s3_endpoint_by_year

    def _save_rides_data_files(self, s3_endpoint_by_year: Dict) -> None:
        """Extracts and save rides data das zip files"""
        
        for year in self.years:
            response = requests.get(s3_endpoint_by_year[year])
            file_name = f"{self.raw_data_directory}/raw_data/rides_data/{year}/{year}_hist_rides_data.zip"
            os.makedirs(os.path.dirname(file_name), exist_ok=True) 
            
            with open(file_name, "wb") as f:
                f.write(response.content)
            f.close()
            logging.info(f'Data for year {year} saved successfully.')
            
        return

    def _retrive_csv_files_path(self) -> Dict[int, List[str]]:
        """Retrive csv files path from the zip files extracted"""
    
        csv_files_path  = {}
        for year in self.years:
            zip_file_object = zipfile.ZipFile(
                f"{self.raw_data_directory}/raw_data/rides_data/{year}/{year}_hist_rides_data.zip"
            )
            csv_files_path[year] = [file for file in zip_file_object.namelist() if search(".*csv*", file)]
            
        return csv_files_path


    @staticmethod
    def _standarize_columns(df: DataFrame, column_name_mapping: Dict[str, str], year: int) -> DataFrame:
        """
        Standarize columns by lowercasing, renaming and enforcing data types based on year selected
        """

        df.columns = [col.lower().strip() for col in df.columns]
        standard_df = df.rename(columns=column_name_mapping)
        
        if all(['start_date' in standard_df, year >= 2022]):
            standard_df = standard_df.assign(
                start_date=to_datetime(standard_df['start_date'], unit='ms'),
                end_date=to_datetime(standard_df['end_date'], unit='ms')
            )
            
        elif all(['start_date' in standard_df, year < 2022]):
            standard_df = standard_df.assign(
                start_date=to_datetime(standard_df['start_date']),
                end_date=to_datetime(standard_df['end_date'])
            )
            
        
        return standard_df
            

    def extract(self) -> None:
        """Extracts and saves rides data available through bixi S3 endpoints"""
        
        try:
            s3_endpoints = self._get_s3_endpoints_metadata()

            logging.info(f'The following s3 endpoints were detected: {s3_endpoints}')
            
            self._save_rides_data_files(
                s3_endpoints
            )
            
            logging.info(f'Data for endpoints {s3_endpoints} was sucessfully extracted and saved.')
                     
            return 
           
        except Exception as e:
            logging.error(f'Extraction was not successfull given the following excpetion: {e}')
                     
            return 
            
    def transform(self) -> DataFrame:
        """
        Combine rides data into a single dataframe with standard columns names and schema
        """
    
        csv_paths = self._retrive_csv_files_path()
        ride_data_columns = [
            'start_date',
            'end_date',
            'name_start_station',
            'name_end_station'
        ]

        dfs = []
        for year in self.years:
            logging.info(f'Transforming data for year {year}.')
            
            zip_file_object = zipfile.ZipFile(
                f"{RAW_DATA_DIRECTORY}/raw_data/rides_data/{year}/{year}_hist_rides_data.zip"
            )
            
            stations_file_path = next(
                (file for file in csv_paths[year] if search('.*[sS]tations.*', file)),
                None
            )

            if year < 2022:
                with zip_file_object.open(f'{stations_file_path}', 'r') as file:
                    stations_dataframe = read_csv(file)
            else:
                stations_dataframe = DataFrame()
            
            rides_files_path = [
                file for file in csv_paths[year] if file != stations_file_path
            ]
            
            rides_dataframes_list = []
            for file_path in rides_files_path:
                with zip_file_object.open(f'{file_path}', 'r') as file:
                    rides_dataframes_list.append(read_csv(file))
                    
            rides_dataframe = concat(rides_dataframes_list)
            
            rides_dataframe = self._standarize_columns(rides_dataframe, self.column_name_mapping, year)
            stations_dataframe = self._standarize_columns(stations_dataframe, self.column_name_mapping, year)
                   
            if year < 2022:
                rides_dataframe_denormalized = (
                    rides_dataframe.merge(
                        stations_dataframe,
                        how='left',
                        left_on='code_start_station',
                        right_on='code'
                    )
                    .merge(
                        stations_dataframe,
                        how='left',
                        left_on='code_end_station',
                        right_on='code',
                        suffixes=('_rides','_station')
                    )
                    .drop(['code_rides','code_station','is_member','duration_sec'], axis=1)
                    .rename(
                        columns={
                            'latitude_rides':'latitude_start_station',
                            'longitude_rides':'longitude_start_station',
                            'latitude_station' :'latitude_end_station',
                            'longitude_station' :'longitude_end_station',
                            'name_rides':'name_start_station',
                            'name_station':'name_end_station'
                        }
                    )
                )
                dfs.append(rides_dataframe_denormalized[self.ride_data_columns])
            
            else:
                dfs.append(rides_dataframe[self.ride_data_columns])
        
        return concat(dfs)
