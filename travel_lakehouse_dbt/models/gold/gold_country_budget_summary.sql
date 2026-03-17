with country_costs as (
    select
        country_id,
        budget_level,
        avg_accommodation_per_night,
        avg_food_per_day,
        avg_transport_per_day,
        avg_entertainment_per_day,
        avg_misc_per_day,
        estimated_total_daily_cost
    from {{ ref('silver_country_costs') }}
),

countries as (
    select
        country_id,
        country_name,
        country_code,
        currency_code,
        currency_name,
        visa_requirement_category,
        avg_safety_score
    from {{ ref('silver_countries') }}
)

select
    co.country_id,
    co.country_name,
    co.country_code,
    co.currency_code,
    co.currency_name,
    co.visa_requirement_category,
    co.avg_safety_score,

    cc.budget_level,
    cc.avg_accommodation_per_night,
    cc.avg_food_per_day,
    cc.avg_transport_per_day,
    cc.avg_entertainment_per_day,
    cc.avg_misc_per_day,
    cc.estimated_total_daily_cost

from country_costs cc
left join countries co
    on cc.country_id = co.country_id