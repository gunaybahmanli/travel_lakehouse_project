with ranked as (
    select
        country_id,
        country_name,
        country_code,
        subregion,
        capital_city,
        currency_code,
        currency_name,
        language_main,
        population,
        area_km2,
        avg_safety_score,
        visa_requirement_category,
        image_url1,
        image_url2,
        country_description,
        fun_fact,
        is_active,
        created_at,
        updated_at,
        source_file,
        bronze_loaded_at,
        row_number() over (
            partition by country_id
            order by bronze_loaded_at desc
        ) as rn
    from default.countries
)

select
    country_id,
    country_name,
    country_code,
    subregion,
    capital_city,
    currency_code,
    currency_name,
    language_main,
    population,
    area_km2,
    avg_safety_score,
    visa_requirement_category,
    image_url1,
    image_url2,
    country_description,
    fun_fact,
    is_active,
    created_at,
    updated_at,
    source_file,
    bronze_loaded_at
from ranked
where rn = 1