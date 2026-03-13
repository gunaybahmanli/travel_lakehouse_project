{% macro register_countries() %}

    {% call statement('create_schema', fetch_result=False)%}
        create schema if not exists default
    {% endcall %}

    {% call statement('drop_countries', fetch_result=False) %}
        drop table if exists default.countries
    {% endcall %}

    {% call statement('create_countries', fetch_result=False) %}
        create table default.countries
        using parquet
        location 's3a://bronze/static/countries/'
    {% endcall %}

{% endmacro %}
