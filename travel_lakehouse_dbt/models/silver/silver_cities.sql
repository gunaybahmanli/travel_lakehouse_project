with ranked as (
    select
        country_id,
        city_id,
        city_name,
        city_name_local,
        population,
        is_capital,
        coastal_flag,
        mountain_flag,
        tourism_score_raw,
        city_description,
        created_at,
        updated_at,
        source_file,
        bronze_loaded_at,
        row_number() over (
            partition by city_id
            order by bronze_loaded_at desc
        ) as rn
    from default.cities
)

select
    country_id,
    city_id,
    city_name,
    city_name_local,
    population,
    is_capital,
    coastal_flag,
    mountain_flag,
    tourism_score_raw,
    city_description,
    created_at,
    updated_at,
    source_file,
    bronze_loaded_at
from ranked
where rn = 1