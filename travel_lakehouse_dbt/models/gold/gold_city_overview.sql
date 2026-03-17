with cities as (
    select * from {{ ref('silver_cities') }}
),

countries as (
    select * from {{ ref('silver_countries') }}
),

attractions as (
    select
        city_id,
        count(*) as attraction_count
    from {{ ref('silver_attractions') }}
    group by city_id
),

restaurants as (
    select
        city_id,
        count(*) as restaurant_count
    from {{ ref('silver_restaurants') }}
    group by city_id 
),

local_food as (
    select
        country_id,
        count(*) as local_food_count
    from {{ ref('silver_local_food') }}
    group by country_id 
)

select
    c.city_id,
    c.city_name,
    c.city_name_local,
    c.country_id,
    co.country_name,
    co.country_code,
    co.capital_city,
    c.population,
    c.is_capital,
    c.coastal_flag,
    c.mountain_flag,
    c.tourism_score_raw,
    c.city_description,

    co.currency_code,
    co.currency_name,
    co.language_main,
    co.avg_safety_score,
    co.visa_requirement_category,

    coalesce(a.attraction_count, 0) as attraction_count,
    coalesce(r.restaurant_count, 0) as restaurant_count,
    coalesce(f.local_food_count, 0) as local_food_count,

    c.created_at,
    c.updated_at,
    c.source_file,
    c.bronze_loaded_at

from cities c 
left join countries co
    on c.country_id = co.country_id
left join attractions a
    on c.city_id = a.city_id
left join restaurants r 
    on c.city_id = r.city_id
left join local_food f 
    on c.country_id = f.country_id
