import datetime
import hashlib
import sys
import requests
import pandas as pd
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# --- CONFIGURATION LAYER ---
PROJECT_ID = "your_gcp_project_id"
STAGING_TABLE = f"{PROJECT_ID}.staging.stg_raw_air_quality"
HYDERABAD_LAT = 17.3850
HYDERABAD_LON = 78.4867
CITY_ID = 1  # Hardcoded matching our seeded dim_cities table

def fetch_raw_api_data():
    """
    Extracts raw hourly air quality data from the Open-Meteo API 
    utilizing connection retries and explicit HTTP error logging.
    """
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={HYDERABAD_LAT}&longitude={HYDERABAD_LON}&hourly=pm2_5"
    
    # Configure an Automatic Retry Strategy for Transient Network Glitches
    # If the API hits a 500, 502, 503, or 504, it backs off and retries up to 3 times.
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # Waits 2s, then 4s, then 8s between retries
        status_forcelist=[500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    
    try:
        print("Sending authenticated request to Open-Meteo API...")
        # 10-second timeout prevents the Cloud Run job from hanging forever and burning budget
        response = session.get(url, timeout=10) 
        
        # Check for HTTP Status Errors (4xx or 5xx)
        response.raise_for_status() 
        return response.json()

    # CASE A: Catch explicit HTTP Status Errors (e.g., 403 Forbidden, 404 Not Found)
    except requests.exceptions.HTTPError as http_err:
        print(f"CRITICAL ERROR: HTTP Exception occurred upstream: {http_err}")
        print(f"API Error Response Payload: {response.text}")
        sys.exit(1)  # Terminate container with Error Code 1 to trigger GCP Alerts

    # CASE B: Catch Network Level Failures (e.g., DNS failure, connection refused)
    except requests.exceptions.ConnectionError as conn_err:
        print(f"CRITICAL ERROR: Network connection failed completely: {conn_err}")
        sys.exit(1)

    # CASE C: Catch Timeout Failures
    except requests.exceptions.Timeout as timeout_err:
        print(f"CRITICAL ERROR: Upstream API timed out after 10 seconds: {timeout_err}")
        sys.exit(1)

    # CASE D: Catch-all for any other weird exception
    except Exception as err:
        print(f"CRITICAL ERROR: An unexpected error occurred during ingestion: {err}")
        sys.exit(1)

def validate_and_transform_row(raw_time, raw_pm25, city_id):
    """
    Enforces the 5 Core Data Validation Gates on a single telemetry event.
    Returns a clean dictionary if valid, or None if a gate trips.
    """
    # GATE 1: Null Value Validation
    if raw_pm25 is None or raw_time is None:
        print(f"Dropped Row: Null value detected at time {raw_time}")
        return None

    # GATE 2: Outlier / Boundary Check
    if raw_pm25 > 1000.0 or raw_pm25 < 0.0:
        print(f"Quarantined Row: Extreme metric anomaly detected ({raw_pm25} μg/m³)")
        return None

    # GATE 3: Data Type & Format Validation
    try:
        clean_time = datetime.datetime.fromisoformat(raw_time)
        clean_pm25 = float(raw_pm25)
        clean_city_id = int(city_id)
    except (ValueError, TypeError) as e:
        print(f"Dropped Row: Data type transformation failure: {e}")
        return None

    # EXTRACTION STEP: Generate the Deterministic Kimball Surrogate Key
    raw_key_string = f"{clean_time.isoformat()}{clean_city_id}"
    fact_key = hashlib.md5(raw_key_string.encode('utf-8')).hexdigest()

    # GATE 4 & 5: Return clean data structured exactly for our BigQuery destination
    return {
        "fact_key": fact_key,
        "reading_time": clean_time,
        "city_id": clean_city_id,
        "pm2_5_value": clean_pm25
    }

def main():
    print("Initializing Air Quality Extraction Pipeline...")
    
    # 1. Fetch raw data with defensive error routing
    raw_payload = fetch_raw_api_data()
    hourly_data = raw_payload.get("hourly", {})
    
    times = hourly_data.get("time", [])
    pm25_values = hourly_data.get("pm2_5", [])
    
    # Defensive programming check: Ensure array dimensions match perfectly
    if len(times) != len(pm25_values):
        print("CRITICAL ERROR: API payload array lengths mismatch.")
        sys.exit(1)

    # 2. Process and Validate records sequentially in memory
    validated_records = []
    for t, val in zip(times, pm25_values):
        clean_record = validate_and_transform_row(t, val, CITY_ID)
        if clean_record:
            validated_records.append(clean_record)
            
    print(f"Validation complete. Ingested rows: {len(times)} | Validated rows: {len(validated_records)}")

    if not validated_records:
        print("WARNING: No valid records found in this batch. Aborting load phase.")
        return

    # 3. Convert clean data array into a structured Pandas DataFrame
    df = pd.DataFrame(validated_records)

    # 4. Initialize BigQuery client and load to Staging Table
    print(f"Connecting to BigQuery... Staging table: {STAGING_TABLE}")
    bq_client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"  # Automatically wipes staging for the next batch load
    )
    
    try:
        # Execute batch load (Free compute ingestion step)
        load_job = bq_client.load_table_from_dataframe(df, STAGING_TABLE, job_config=job_config)
        load_job.result()  # Wait for the commit to complete
        print("Step 1 Complete: Staging table successfully loaded.")
        print("Pipeline execution complete. Ready for target MERGE execution.")
    except Exception as bq_err:
        print(f"CRITICAL ERROR: Failed to load data to BigQuery staging: {bq_err}")
        sys.exit(1)

if __name__ == "__main__":
    main()