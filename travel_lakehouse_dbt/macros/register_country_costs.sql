{% macro register_country_costs() %}

  {% call statement('create_schema', fetch_result=False) %}
    create schema if not exists default
  {% endcall %}

  {% call statement('drop_table', fetch_result=False) %}
    drop table if exists default.country_costs
  {% endcall %}

  {% call statement('create_table', fetch_result=False) %}
    create table default.country_costs
    using parquet
    location 's3a://bronze/static/country_costs/'
  {% endcall %}

{% endmacro %}