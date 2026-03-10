from pyspark.sql.functions import col, when, to_timestamp, round

# --- 1. LOAD THE RAW DATA ---
# We're grabbing that raw data we just saved in the Bronze layer.
df_bronze = spark.read.table("smart_grid.bronze.raw_weather_forecast")

# --- 2. CLEANING AND FIXING ---
# We want to rename things so they make sense to a human and 
# make sure the dates are in a format the computer understands.
df_silver = df_bronze.select(
    to_timestamp(col("time")).alias("time_window"),
    col("wind_speed_100m").alias("wind_speed"),
    col("shortwave_radiation").alias("solar_power"),
    
    # --- ADDING THE 'SMART' PART ---
    # We're creating a 'Price' column based on the weather.
    # Logic: If it's windy, energy is cheap ($30). If it's sunny, it's ($35). 
    # Otherwise, it's expensive ($50).
    round(
        when(col("wind_speed_100m") > 8, 30)
        .when(col("shortwave_radiation") > 400, 35)
        .otherwise(50), 2
    ).alias("energy_cost"),
    
    # We also create a 'Green Score' to show how eco-friendly the hour is.
    round((col("wind_speed_100m") * 2) + (col("shortwave_radiation") * 0.1), 0).alias("eco_score")
)

# --- 3. SAVE THE CLEAN DATA ---
# Now we save this to the 'Silver' layer. It's clean, labeled, and ready to use!
(df_silver.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("smart_grid.silver.refined_energy_data"))

print("Step 2 Done: Silver layer is clean and enriched with price data!")
