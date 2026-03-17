with food_stats as (
    select
        country_id,
        count(*) as total_food_count,
        avg(popularity_score_raw) as avg_popularity_score,
        avg(cast(avg_price_local as double)) as avg_price,
        sum(case when is_vegetarian = true then 1 else 0 end) as vegetarian_count,
        sum(case when is_spicy = true then 1 else 0 end) as spicy_count
    from {{ ref('silver_local_food') }}
    group by country_id
),

countries as (
    select
        country_id,
        country_name,
        country_code,
        currency_code,
        currency_name
    from {{ ref('silver_countries') }}
)

select
    co.country_id,
    co.country_name,
    co.country_code,
    co.currency_code,
    co.currency_name,

    coalesce(f.total_food_count, 0) as total_food_count,
    round(coalesce(f.avg_popularity_score, 0), 2) as avg_popularity_score,
    round(coalesce(f.avg_price, 0), 2) as avg_price,

    coalesce(f.vegetarian_count, 0) as vegetarian_count,
    coalesce(f.spicy_count, 0) as spicy_count

from countries co
left join food_stats f
    on co.country_id = f.country_id