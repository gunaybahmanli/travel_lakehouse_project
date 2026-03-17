with city_overview as (
    select * from {{ ref('gold_city_overview') }}
),

weather_latest as (
    select * from {{ ref('gold_weather_latest') }}
),

attractions_summary as (
    select * from {{ ref('gold_city_attractions_summary') }}
),

country_budget as (
    select * from {{ ref('gold_country_budget_summary') }}
),

food_summary as (
    select * from {{ ref('gold_city_food_summary') }}
)

select
    co.city_id,
    co.city_name,
    co.city_name_local,
    co.country_id,
    co.country_name,
    co.country_code,
    co.capital_city,
    co.population,
    co.is_capital,
    co.coastal_flag,
    co.mountain_flag,
    co.tourism_score_raw,
    co.city_description,

    co.currency_code,
    co.currency_name,
    co.language_main,
    co.avg_safety_score,
    co.visa_requirement_category,

    co.attraction_count,
    co.restaurant_count,
    co.local_food_count,

    ats.avg_attraction_rating,
    ats.total_attraction_reviews,
    ats.paid_attraction_count,
    ats.family_friendly_attraction_count,

    w.current_temp_c,
    w.current_feelslike_c,
    w.current_condition_text,
    w.current_wind_kph,
    w.current_humidity,
    w.current_cloud,
    w.current_uv,
    w.current_precip_mm,
    w.current_vis_km,
    w.forecast_date,
    w.forecast_maxtemp_c,
    w.forecast_mintemp_c,
    w.forecast_avgtemp_c,
    w.daily_chance_of_rain,
    w.forecast_condition_text,
    w.sunrise,
    w.sunset,

    cb.budget_level,
    cb.avg_accommodation_per_night,
    cb.avg_food_per_day,
    cb.avg_transport_per_day,
    cb.avg_entertainment_per_day,
    cb.avg_misc_per_day,
    cb.estimated_total_daily_cost,

    fs.total_food_count,
    fs.avg_popularity_score,
    fs.avg_price,
    fs.vegetarian_count,
    fs.spicy_count,

    co.created_at,
    co.updated_at,
    co.source_file,
    co.bronze_loaded_at

from city_overview co
left join attractions_summary ats
    on co.city_id = ats.city_id
left join weather_latest w
    on lower(co.city_name) = lower(w.requested_city)
left join country_budget cb
    on co.country_id = cb.country_id
left join food_summary fs
    on co.country_id = fs.country_id