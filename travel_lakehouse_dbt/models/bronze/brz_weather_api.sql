{{ config(materialized='view') }}

select *
from parquet.`s3a://bronze/weather/daily_forecast/`