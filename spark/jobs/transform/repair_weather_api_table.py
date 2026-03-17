from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("repair-weather-api-table")
    .getOrCreate()
)

spark.sql("MSCK REPAIR TABLE default.weather_api")

print("default.weather_api repaired successfully.")

spark.stop()