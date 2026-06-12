import requests
import pandas as pd

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




    

print("Text file written successfully!")
print(df)