{% macro register_weather_api() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_weather_api', fetch_result=False) %}
    drop table if exists default.weather_api
  {% endcall %}

  {% call statement('create_weather_api', fetch_result=False) %}
    create table default.weather_api
    using parquet
    location 's3a://bronze/weather/daily_forecast/'
  {% endcall %}

{% endmacro %}