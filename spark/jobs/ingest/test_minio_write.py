from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-minio-write")
    .getOrCreate()
)

input_path = "file:///opt/spark/data/raw/static/countries.csv"
output_path = "s3a://raw/static/countries/"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

df.show(5, truncate=False)

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Write to MinIO completed successfully.")

spark.stop()