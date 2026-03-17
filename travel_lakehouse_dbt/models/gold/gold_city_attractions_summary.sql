with attraction_stats as (
    select
        city_id,
        city_name,
        country_id,
        count(*) as attraction_count,
        avg(rating) as avg_attraction_rating,
        sum(reviews) as total_attraction_reviews,
        sum(case when entry_fee > 0 then 1 else 0 end) as paid_attraction_count,
        sum(case when family = true then 1 else 0 end) as family_friendly_attraction_count
    from {{ ref('silver_attractions') }}
    group by
        city_id,
        city_name,
        country_id
),

cities as (
    select
        city_id,
        city_name,
        city_name_local,
        country_id
    from {{ ref('silver_cities') }}
),

countries as (
    select
        country_id,
        country_name,
        country_code
    from {{ ref('silver_countries') }}
)

select
    c.city_id,
    c.city_name,
    c.city_name_local,
    c.country_id,
    co.country_name,
    co.country_code,

    coalesce(a.attraction_count, 0) as attraction_count,
    round(coalesce(a.avg_attraction_rating, 0), 2) as avg_attraction_rating,
    coalesce(a.total_attraction_reviews, 0) as total_attraction_reviews,
    coalesce(a.paid_attraction_count, 0) as paid_attraction_count,
    coalesce(a.family_friendly_attraction_count, 0) as family_friendly_attraction_count

from cities c
left join attraction_stats a
    on c.city_id = a.city_id
left join countries co
    on c.country_id = co.country_id