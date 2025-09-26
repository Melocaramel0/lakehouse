\
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

BASE = Path(__file__).resolve().parents[1]
raw_dir = BASE / "data" / "raw"
processed_dir = BASE / "data" / "processed"

spark = (SparkSession.builder
         .appName("Lakehouse-Transform")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

df = spark.read.option("header", True).csv(str(raw_dir) + "/*/*.csv")

# Cast & clean
df2 = (df
       .withColumn("event_time", to_timestamp("event_time"))
       .withColumn("event_date", to_date(col("event_time")))
       .withColumn("amount", col("amount").cast("double"))
       .dropna(subset=["event_time", "user_id", "category", "amount"])
      )

# Write Parquet partitioned by event_date
(df2
 .repartition(1)  # keep files small for the exercise
 .write
 .mode("overwrite")
 .partitionBy("event_date")
 .parquet(str(processed_dir)))

print(f"Wrote Parquet to {processed_dir}")
spark.stop()
