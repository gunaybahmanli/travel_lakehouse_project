from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("register-weather-bronze-table")
    .getOrCreate()
)


spark.sql(
    """
    CREATE DATABASE IF NOT EXISTS default
    """
)

spark.sql(
    """
    DROP TABLE IF EXISTS default.weather_api
    """
)

spark.sql(
    """
    CREATE TABLE default.weather_api
    USING PARQUET
    LOCATION 's3a://bronze/weather/daily_forecast/'
    """
)

spark.sql(
    """
    MSCK REPAIR TABLE default.weather_api
    """
)

print("\nExternal table default.weather_api created and partitions repaired successfully.")

spark.stop()