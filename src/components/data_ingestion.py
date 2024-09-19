from sqlalchemy import create_engine
import urllib.request as request
import pandas as pd
import os
import configparser
config = configparser.RawConfigParser()
from src.constants import *


STAGE_NAME = "Data Ingestion"

class DataIngestion:       

    def __init__(self):
        self.config = config.read(CONFIG_FILE_PATH)
    
    def download_dataframe(self, filename):
        print(config.get('DATA', filename))        
        df = pd.read_csv(config.get('DATA', filename))
        return df


    def upload_dataframe(self, dataframe, table_name, if_exists='replace'):
        try:
            connection_string = config.get('DATA', 'connection_string')
            engine = create_engine(connection_string)

            dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False)

            print(f"Dataframe successfully uploaded to table '{table_name}'")

        except Exception as error:
            print(f"Error uploading dataframe to table '{table_name}': {error}")


    def download_files(self):
        file_list = config.get('DATA', 'files').split(',')

        for file in file_list:
            df = self.download_dataframe(file)
            self.upload_dataframe(df, file.replace("_url", ""))

            
    def fetch_data(self):
        # Route dataframes
        route_df = self.fetch_table("td_raw_data", "routes_table")
        route_weather_df = self.fetch_table("td_raw_data", "routes_weather")
        traffic_table_df = self.fetch_table("td_raw_data", "traffic_table")

        # Truck dataframes
        trucks_df = self.fetch_data("td_raw_data", "trucks_table")
        drivers_table = self.fetch_data("td_raw_data", "drivers_table")
        truck_schedule_df = self.fetch_data("td_raw_data", "truck_schedule_table")


    def fetch_table(database, table):
        try:
            user = "postgres"
            password = "password"
            host = "localhost"
            port = 5432

            connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
            engine = create_engine(connection_string)

            query = f"SELECT * FROM {table};"

            df = pd.read_sql(query, engine)

            return df
        
        except Exception as error:
            print(f"Error fetching data from table '{table}' in database '{database}': {error}")
            return None
    
  