from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, current_timestamp, lit
import sys


def get_processOdate() -> str:
    if len(sys.argv) < 2:
        raise ValueError("Please provide the process date as an argument in the format YYYY-MM-DD.")
    return sys.argv[1]

process_date = get_processOdate()

spark = (
    SparkSession.builder
    .appName(f"weather-raw-to-bronze-{process_date}")
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

raw_path = f"s3a://raw/api/weather/extraction_date={process_date}/*/*/*.json"
bronze_path = "s3a://bronze/weather/daily_forecast/"

df_raw = (
    spark.read
    .option("multiLine", True)
    .json(raw_path)
    .withColumn("source_file", input_file_name())
)




if df_raw.rdd.isEmpty():
    raise ValueError(f"No raw data found for the process date: {process_date} at path: {raw_path}")

df_weather = (
    df_raw
    .filter(col("_corrupt_record").isNull())
    .withColumn("forecast_day", explode(col("response.forecast.forecastday")))
    .select(
        col("metadata.extraction_date").alias("extraction_date"),
        col("metadata.extraction_timestamp").alias("extraction_timestamp"),
        col("metadata.country_name").alias("country_name"),
        col("metadata.country_code").alias("country_code"),
        col("metadata.capital_city").alias("capital_city"),
        col("metadata.requested_city").alias("requested_city"),
        col("metadata.source").alias("source"),

        col("response.location.name").alias("location_name"),
        col("response.location.country").alias("location_country"),
        col("response.location.region").alias("location_region"),
        col("response.location.lat").alias("location_lat"),
        col("response.location.lon").alias("location_lon"),
        col("response.location.localtime").alias("location_localtime"),
        col("response.location.tz_id").alias("location_tz_id"),

        col("response.current.temp_c").alias("current_temp_c"),
        col("response.current.feelslike_c").alias("current_feelslike_c"),
        col("response.current.condition.text").alias("current_condition_text"),
        col("response.current.wind_kph").alias("current_wind_kph"),
        col("response.current.humidity").alias("current_humidity"),
        col("response.current.cloud").alias("current_cloud"),
        col("response.current.uv").alias("current_uv"),
        col("response.current.precip_mm").alias("current_precip_mm"),
        col("response.current.vis_km").alias("current_vis_km"),

        col("forecast_day.date").alias("forecast_date"),
        col("forecast_day.day.maxtemp_c").alias("forecast_maxtemp_c"),
        col("forecast_day.day.mintemp_c").alias("forecast_mintemp_c"),
        col("forecast_day.day.avgtemp_c").alias("forecast_avgtemp_c"),
        col("forecast_day.day.avghumidity").alias("forecast_avghumidity"),
        col("forecast_day.day.maxwind_kph").alias("forecast_maxwind_kph"),
        col("forecast_day.day.totalprecip_mm").alias("forecast_totalprecip_mm"),
        col("forecast_day.day.daily_chance_of_rain").alias("daily_chance_of_rain"),
        col("forecast_day.day.daily_will_it_rain").alias("daily_will_it_rain"),
        col("forecast_day.day.condition.text").alias("forecast_condition_text"),

        col("forecast_day.astro.sunrise").alias("sunrise"),
        col("forecast_day.astro.sunset").alias("sunset"),
        col("forecast_day.astro.moonrise").alias("moonrise"),
        col("forecast_day.astro.moonset").alias("moonset"),
        col("forecast_day.astro.moon_phase").alias("moon_phase"),
        col("forecast_day.astro.moon_illumination").alias("moon_illumination"),

        col("source_file")
    )
    .withColumn("partition_extraction_date", lit(process_date))
    .withColumn("bronze_loaded_at", current_timestamp())
)


print(f"Processing weather data for extraction date: {process_date}")
print(f"Input raw data path: {raw_path}")

df_weather.printSchema()
df_weather.show(10, truncate=False)

(
    df_weather
    .write
    .mode("overwrite")
    .partitionBy("partition_extraction_date")
    .parquet(bronze_path)
)

print(f"Weather data successfully transformed from raw to bronze layer for extraction date: {process_date}")


spark.stop()    