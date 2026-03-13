select
    extraction_date,
    extraction_timestamp,
    country_name,
    country_code,
    capital_city,
    requested_city,
    source,

    location_name,
    location_country,
    location_region,
    location_lat,
    location_lon,
    location_localtime,
    location_tz_id,

    current_temp_c,
    current_feelslike_c,
    current_condition_text,
    current_wind_kph,
    current_humidity,
    current_cloud,
    current_uv,
    current_precip_mm,
    current_vis_km,

    forecast_date,
    forecast_maxtemp_c,
    forecast_mintemp_c,
    forecast_avgtemp_c,
    forecast_avghumidity,
    forecast_maxwind_kph,
    forecast_totalprecip_mm,
    daily_chance_of_rain,
    daily_will_it_rain,
    forecast_condition_text,

    sunrise,
    sunset,
    moonrise,
    moonset,
    moon_phase,
    moon_illumination,

    partition_extraction_date,
    bronze_loaded_at
    
from {{ source('bronze', 'weather_api') }}