{% macro register_local_food() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_table', fetch_result=False) %}
    drop table if exists default.local_foods
  {% endcall %}

  {% call statement('create_table', fetch_result=False) %}
    create table default.local_foods
    using parquet
    location 's3a://bronze/static/local_foods/'
  {% endcall %}

{% endmacro %}