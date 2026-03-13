{% macro register_cities() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_cities', fetch_result=False) %}
    drop table if exists default.cities
  {% endcall %}

  {% call statement('create_cities', fetch_result=False) %}
    create table default.cities
    using parquet
    location 's3a://bronze/static/cities/'
  {% endcall %}

{% endmacro %}