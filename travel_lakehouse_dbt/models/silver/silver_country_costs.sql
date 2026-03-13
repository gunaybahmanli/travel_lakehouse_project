select
    cost_id,
    country_id,
    budget_level,
    avg_accommodation_per_night,
    avg_food_per_day,
    avg_transport_per_day,
    avg_entertainment_per_day,
    avg_misc_per_day,
    estimated_total_daily_cost,
    source_file,
    bronze_loaded_at
from default.country_costs