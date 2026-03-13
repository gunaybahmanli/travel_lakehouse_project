{% macro register_attractions() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_attractions', fetch_result=False) %}
    drop table if exists default.attractions
  {% endcall %}

  {% call statement('create_attractions', fetch_result=False) %}
    create table default.attractions
    using parquet
    location 's3a://bronze/static/attractions/'
  {% endcall %}

{% endmacro %}