select
    country_id,
    food_id,
    food_name,
    food_category,
    short_description,
    avg_price_local,
    currency,
    is_vegetarian,
    is_spicy,
    popularity_score_raw,
    created_at,
    updated_at,
    source_file,
    bronze_loaded_at
from default.local_food