import requests
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit

# --- DATA SOURCE CONFIGURATION ---
# We're targeting West Texas coordinates (32.5, -100.0) as it's a major hub for 
# wind and solar energy production. This makes our "Smart Grid" use case realistic.
LAT, LON = 32.5, -100.0 
BASE_URL = "https://api.open-meteo.com/v1/forecast"

# We request specific weather features that directly impact renewable energy yield:
# 1. Wind Speed at 100m (the height of industrial turbines)
# 2. Shortwave Radiation (for solar panel efficiency)
# 3. Temperature (to account for grid load/heating demand)
api_params = {
    "latitude": LAT,
    "longitude": LON,
    "hourly": ["temperature_2m", "wind_speed_100m", "shortwave_radiation"],
    "timezone": "UTC"
}

# --- INGESTION PHASE: FETCH RAW DATA ---
# Using the requests library to pull the live 7-day forecast.
# We keep the raw JSON format initially to ensure we have a 'snapshot' of the 
# response exactly as the API provided it.
try:
    response = requests.get(BASE_URL, params=api_params)
    response.raise_for_status() # Ensure we catch any 4xx/5xx errors early
    raw_data_json = response.json()
except Exception as e:
    print(f"Error fetching data from API: {e}")
    raise

# --- BRONZE LAYER PREPARATION: THE 'RAW' LANDING ---
# In a Medallion architecture, the Bronze layer should be an exact replica of source data.
# We use Pandas as a bridge here for easy JSON flattening before moving to Spark 
# for big-data scalability.
hourly_dict = raw_data_json['hourly']
pdf_raw = pd.DataFrame(hourly_dict)
spark_df = spark.createDataFrame(pdf_raw)

# --- DATA LINEAGE & AUDITABILITY ---
# Adding metadata is a 'Senior' move. It ensures that if we find a bug in 6 months,
# we know exactly when this specific record was ingested and where it came from.
# This is crucial for data governance and debugging.
spark_df = (spark_df
            .withColumn("ingested_at", current_timestamp())
            .withColumn("data_source", lit("Open-Meteo-API-Weather"))
            .withColumn("region", lit("West-Texas-Hub")))

# --- PERSISTENCE: SAVING TO DELTA LAKE ---
# We save this as a Delta table in our 'smart_grid' catalog. 
# Using Delta format provides ACID transactions and 'Time Travel' (versioning).
# We use 'overwrite' for this demo, but in a production environment, 
# we would likely switch to 'append' for historical tracking.
(spark_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("smart_grid.bronze.raw_weather_forecast"))

print("Successfully landed raw data into Bronze Table: smart_grid.bronze.raw_weather_forecast")
