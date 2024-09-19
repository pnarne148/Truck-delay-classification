import pandas as pd
import os.path as path
import os
import sys

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
sys.path.append(parent_directory)

from src.components.data_ingestion import DataIngestion

STAGE_NAME = "Data Ingestion"
ingestion_obj = DataIngestion()

class DataIngestionPipeline:
    def __init__(self):
        pass

    def main(self):
        try:        
            # ingestion_obj.fetch_data()
            ingestion_obj.download_files()
        except Exception as e:
            raise e


    
if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :",STAGE_NAME)
        obj = DataIngestionPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e