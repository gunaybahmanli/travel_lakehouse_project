{% macro repair_weather_api() %}

  {% call statement('repair_weather_api', fetch_result=False) %}
    msck repair table default.weather_api
  {% endcall %}

{% endmacro %}