##Implement the ETL pipeline, wherein we will read the dataset from our source and convert it into json and then push it into the MongoDB

import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL=os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)
##Certifi is a python package that provides a set of root certificates.
## It is commonly used by python libraries that need to make a secure http connection
##Rn we are trying to make a http connection to MongoDB database, and we would be using libraries like request,
##This is just to ensure that only the certificate verified by the trusted certified authorities
##So whenever we are trying to communicate with mongoDB and this libary has been imported, it knows it is a valid request being made 
import certifi
ca=certifi.where()
##This line retrives the part to the bundle of CA certificates provided by certifi and store it in the variable ca
##CA - trusted certificate authorities, 
##usually done for SSL or TLS connection to verify that that the server you are connecting to has trusted certificates
import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

class NetworkDataExtract():  ##this will be the etl pipeline
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def csv_to_json_convertor(self,file_path):
        try:
            data=pd.read_csv(file_path)
            ##when we read the dataset, by default there be an index applied so we need to drop that index.
            data.reset_index(drop=True,inplace=True)
            records=list(json.loads(data.T.to_json()).values()) ##list of json arrays
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def insert_data_mongodb(self,records,database,collection):
        try:
            self.database=database
            self.collection=collection
            self.records=records

            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)##we need the client to connect to MongoDB,and this client needs to be initialized with pymongo
            ##pymongo is library that is used along with python to follow python programming to connect to MongoDB
            ##We are going to assign this particular Mongo client what database we are using
            self.database = self.mongo_client[self.database]
            
            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e,sys)

##Execution of ETL pipeline
if __name__=='__main__':
    FILE_PATH="Network_Data\phisingData.csv"
    DATABASE="VINAYAI"
    Collection="NetworkData"
    networkobj=NetworkDataExtract()
    records=networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    print(records)
    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(no_of_records)
        


