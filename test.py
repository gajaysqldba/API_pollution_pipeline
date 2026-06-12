import os
import requests
import pandas as pd
from google.cloud import storage

API = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=17.3850&longitude=78.4867&hourly=pm2_5&timezone=Asia%2FKolkata&forecast_days=1"
#GCP project
PROJECT_ID = "bqdemo-496217"
#GCP bucket name
BUCKET_NAME = "bqdemo-496217-bucket"
#GCP blob name
LOCAL_CSV_FILE = "air_quality_data.csv" 


response = requests.get(API).json()
hourly_dict = response["hourly"]
df = pd.DataFrame(hourly_dict)
df.to_csv(LOCAL_CSV_FILE,index= False)

def upload_to_gcs_lake():
    '''Ship the csv file to the GCS bucket for storage'''
    #creating the clinet for storage and connecting to the GCP project
    storage_client = storage.Client(project=PROJECT_ID)
    #getting the bucket from storage client
    bucket =storage_client.bucket(BUCKET_NAME)
    #creating the blob in the bucket by proving the path and the name of the file to be stored
    blob = bucket.blob("landing_zone/air_quality_data.csv")
    #uploading the local csv file to the blob in the bucket
    blob.upload_from_filename(LOCAL_CSV_FILE)
    print(f"File {LOCAL_CSV_FILE} uploaded to {BUCKET_NAME} successfully!" )

    if os.path.exists(LOCAL_CSV_FILE):
        os.remove(LOCAL_CSV_FILE)

if __name__ == "__main__":
    upload_to_gcs_lake()
        





    

print("Text file written successfully!")
print(df)