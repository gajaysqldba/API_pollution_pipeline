import requests
import pandas as pd

API = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=17.3850&longitude=78.4867&hourly=pm2_5&timezone=Asia%2FKolkata&forecast_days=1"

response = requests.get(API).json()
hourly_dict = response["hourly"]
df = pd.DataFrame(hourly_dict)



#with open("output.txt", "w", encoding="utf-8") as file:
    #file.write(df.to_string(index=False))
    

#print("Text file written successfully!")
print(df)