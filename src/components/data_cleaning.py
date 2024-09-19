from sqlalchemy import create_engine
import hopsworks

import urllib.request as request
import pandas as pd
import os
import configparser
config = configparser.RawConfigParser()
from src.constants import *


STAGE_NAME = "Data Cleaning"

class DataCleaning:
    
    def __init__(self):
        self.config = config.read(CONFIG_FILE_PATH)

    def fetch_table(self, table):
        try:
            connection_string = config.get('DATA', "connection_string")
            engine = create_engine(connection_string)

            query = f"SELECT * FROM {table};"

            df = pd.read_sql(query, engine)
            return df
        
        except Exception as error:
            print(f"Error fetching data from table '{table}': {error}")
            return None   

    # Function to classify condition and intensity
    def classify_weather(self, description):
        intensity_mapping = {
            1: ['Light', 'Patchy light'],
            2: ['Moderate', 'Patchy moderate', 'Moderate or heavy'],
            3: ['Heavy', 'Torrential'],
            4: ['with thunder', 'Thundery outbreaks possible'],
        }
        
        condition_mapping = {
            'Snow': ['snow', 'Blowing snow', 'Freezing drizzle', 'sleet'],
            'Rain': ['drizzle', 'rain', 'Rain shower'],
            'Fog': ['fog'],
            'Cloudy': ['Cloudy', 'Overcast'],
            'Clear': ['Clear', 'Sunny'],
            'Thunderstorms': ['thunder', 'Thundery outbreaks'],
            'Ice': ['ice', 'freezing', 'Ice pellets']
        }
        
        intensity = 0
        condition = 'Unknown'  # Default condition if not explicitly found
        
        for key, values in intensity_mapping.items():
            if any(val.lower() in description.lower() for val in values):
                intensity = key
                break
        
        for key, values in condition_mapping.items():
            if any(val.lower() in description.lower() for val in values):
                condition = key
                break
        
        return pd.Series([condition, intensity])
    
    def create_feature_group(self, project, dataframe, name, key):
        print(f"Uploading '{name}'")

        fs = project.get_feature_store()

        feature_group = fs.create_feature_group(
            name=name,
            version=1,
            primary_key=key,  
            online_enabled=False  
        )

        feature_group.insert(dataframe)

