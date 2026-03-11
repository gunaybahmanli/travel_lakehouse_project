from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .appName("Load Static CSV to Raw")
    .getOrCreate()
)

base_input = "file:///opt/spark/data/raw/static/"
base_output = "s3a://raw/static/"

datasets = [
    "countries",
    "cities",
    "attractions",
    "country_costs",
    "local_foods",
    "restaurants"
]

for table in datasets:
    input_path = f"{base_input}{table}.csv"
    output_path = f"{base_output}{table}/"

    print(f"Processing {table}...")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )


    print(f"Finished processing {table}.")

spark.stop()