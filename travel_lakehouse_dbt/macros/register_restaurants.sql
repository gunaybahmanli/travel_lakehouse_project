{% macro register_restaurants() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_table', fetch_result=False) %}
    drop table if exists default.restaurants
  {% endcall %}

  {% call statement('create_table', fetch_result=False) %}
    create table default.restaurants
    using parquet
    location 's3a://bronze/static/restaurants/'
  {% endcall %}

{% endmacro %}