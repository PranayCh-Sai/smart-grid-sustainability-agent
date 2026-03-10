from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# GET THE CLEAN DATA 
df_silver = spark.read.table("smart_grid.silver.refined_energy_data")

# RANKING THE BEST HOURS
# We want to find the top 5 cheapest and greenest hours.
# We tell the computer to order them by price (lowest first).
ranking_rule = Window.orderBy(col("energy_cost").asc(), col("eco_score").desc())

# PICKING THE TOP 5 
# We give each hour a 'Rank' and keep only the ones ranked 1 to 5.
df_gold = (df_silver
           .withColumn("rank", row_number().over(ranking_rule))
           .filter(col("rank") <= 5)
           .select("rank", "time_window", "energy_cost", "eco_score"))

#  RESULT 
# This table is for the bosses. It tells them exactly when to run the factory.
(df_gold.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("smart_grid.gold.factory_schedule"))

print("Step 3 Done: Gold layer is ready for the AI Agent to read!")
