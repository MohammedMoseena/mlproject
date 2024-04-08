import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

from sklearn.model_selection import train_test_split
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts',"train.csv")
    test_data_path: str=os.path.join('artifacts',"test.csv")
    raw_data_path: str=os.path.join('artifacts',"data.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config=DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion method or component")
        try:
            # you can get the data from csv,database or API
            # df=pd.read_csv('notebook\data\stud.csv') #If data is in csv format
            # Connect to the the MYSQL database
            load_dotenv()
            mydb=mysql.connector.connect(
                host = os.environ.get('MYSQL_HOST'),
                user = os.environ.get('MYSQL_USER'),
                password = os.environ.get('MYSQL_PASSWORD'),
                database = os.environ.get('MYSQL_DATABASE')
            )
            
            mycursor = mydb.cursor()

            # Retrieve the data from the database
            sql = "SELECT * FROM stud"  # Replace with your actual table name
            mycursor.execute(sql)
            data = mycursor.fetchall()

            # Create a DataFrame from the fetched data
            df = pd.DataFrame(data, columns=mycursor.column_names)

            logging.info('Read the dataset as dataframe')

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path,index=False,header=True)

            logging.info("Train test split initiated")
            train_set,test_set=train_test_split(df,test_size=0.2,random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path,index=False,header=True)

            test_set.to_csv(self.ingestion_config.test_data_path,index=False,header=True)

            logging.info("Ingestion of the data is completed")

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path

            )
        except Exception as e:
            raise CustomException(e,sys)
        
if __name__=="__main__":
    obj=DataIngestion()
    obj.initiate_data_ingestion()

    