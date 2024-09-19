
import configparser
import hopsworks
config = configparser.RawConfigParser()
import os.path as path
import pandas as pd
import sys
import os

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
sys.path.append(parent_directory)

from src.components.data_cleaning import DataCleaning

STAGE_NAME = "Data Cleaning"

cleaning_obj=DataCleaning()

class DataCleaningPipeline:
    def __init__(self):
        pass

    def main(self):
        try:
            
            route_df = cleaning_obj.fetch_table("routes_table")
            route_weather_df = cleaning_obj.fetch_table("routes_weather")
            traffic_table_df = cleaning_obj.fetch_table("traffic_table")
            trucks_df = cleaning_obj.fetch_table("trucks_table")
            drivers_df = cleaning_obj.fetch_table("drivers_table")
            trucks_schedule_df = cleaning_obj.fetch_table("truck_schedule_table")
            

            ### DATA CLEANING ###

            #############################################################
            # Route table

            #############################################################
            # Route weather table
            route_weather_df['Date'] = pd.to_datetime(route_weather_df['Date'], errors='coerce')
            route_weather_df[['condition', 'intensity']] = route_weather_df['description'].apply(cleaning_obj.classify_weather)
            route_weather_df.drop(['description', 'chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder'], axis=1, inplace=True)

            #############################################################
            # Trucks table
            trucks_df.fillna({'load_capacity_pounds': trucks_df['load_capacity_pounds'].mean()}, inplace=True)
            trucks_df['fuel_type'] = trucks_df['fuel_type'].replace('', 'gas')

            #############################################################
            # Drivers table
            drivers_df['driving_style'] = drivers_df['driving_style'].replace('', 'proactive')
            drivers_df['experience'] = drivers_df['experience'].apply(abs)
            
            #############################################################
            # Route traffic table
            traffic_table_df['date'] = pd.to_datetime(traffic_table_df['date'])
            mean_values = traffic_table_df.groupby(['route_id', 'hour'])['no_of_vehicles'].transform('mean')
            traffic_table_df['no_of_vehicles'] = traffic_table_df['no_of_vehicles'].fillna(mean_values)

            #############################################################
            # Trucks schedule table
            trucks_schedule_df['departure_date'] = pd.to_datetime(trucks_schedule_df['departure_date'])
            trucks_schedule_df['estimated_arrival'] = pd.to_datetime(trucks_schedule_df['estimated_arrival'])

            #############################################################
            cleaning_obj.create_feature_group(route_df, "routes_info", ['route_id'])
            cleaning_obj.create_feature_group(route_weather_df, "routes_weather_info", ['route_id',	'Date'])
            cleaning_obj.create_feature_group(drivers_df, "drivers_info", ['driver_id'])
            cleaning_obj.create_feature_group(trucks_df, "trucks_info", ['truck_id'])
            cleaning_obj.create_feature_group(traffic_table_df, "route_traffic_info", ['route_id', 'date', 'hour'])
            cleaning_obj.create_feature_group(trucks_schedule_df, "truck_schedule_info", ['truck_id','route_id', 'departure_date','estimated_arrival'])

        except Exception as e:
            raise e


    
if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :",STAGE_NAME)
        obj = DataCleaningPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e