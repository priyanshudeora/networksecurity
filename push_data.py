import os
import sys
import json
import certifi
import pandas as pd
import pymongo
from dotenv import load_dotenv
from networksecurity.exception.exception import NetworkSecurityException

# --- Setup ---
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
# Get the certificate path
ca = certifi.where()


class NetworkDataExtract:
    """
    A class to extract data from a CSV, convert it, and load it into MongoDB.
    """
    def __init__(self):
        """
        Initializes the MongoDB client. The client is created once and reused.
        """
        try:
            # 1. Initialize client once and use the certificate for secure connection
            self.client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path: str) -> list:
        """
        Reads a CSV file and converts it into a list of dictionaries (JSON records).
        """
        try:
            data = pd.read_csv(file_path)
            # 2. Simplify the conversion using .to_dict()
            records = data.to_dict(orient='records')
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records: list, db_name: str, collection_name: str) -> int:
        """
        Inserts a list of records into the specified MongoDB database and collection.
        """
        try:
            # 3. Reuse the client connection from __init__
            database = self.client[db_name]
            collection = database[collection_name]
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == '__main__':
    FILE_PATH = r"Network_Data\phisingData.csv" # Using raw string for Windows paths
    DATABASE = "KRISHAI"
    COLLECTION = "NetworkData"

    network_obj = NetworkDataExtract()

    print("Converting CSV to JSON records...")
    json_records = network_obj.csv_to_json_convertor(file_path=FILE_PATH)
    print(f"Successfully converted {len(json_records)} records.")

    print("Inserting records into MongoDB...")
    num_inserted = network_obj.insert_data_mongodb(
        records=json_records,
        db_name=DATABASE,
        collection_name=COLLECTION
    )
    print(f"Successfully inserted {num_inserted} records into collection '{COLLECTION}'.")