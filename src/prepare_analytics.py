\
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count as _count

BASE = Path(__file__).resolve().parents[1]
processed_dir = BASE / "data" / "processed"
prepared_dir = BASE / "data" / "prepared"

spark = (SparkSession.builder
         .appName("Lakehouse-Prepare")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

df = spark.read.parquet(str(processed_dir))

# Aggregates
by_user = (df.groupBy("user_id")
             .agg(_count("*").alias("events"),
                  _sum("amount").alias("total_amount")))

by_cat = (df.groupBy("category")
            .agg(_count("*").alias("events"),
                 _sum("amount").alias("total_amount")))

# Save
by_user.repartition(1).write.mode("overwrite").parquet(str(prepared_dir / "by_user"))
by_cat.repartition(1).write.mode("overwrite").parquet(str(prepared_dir / "by_category"))

print(f"Prepared analytics under {prepared_dir}")
spark.stop()
