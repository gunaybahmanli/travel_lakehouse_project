import streamlit as st
import pandas as pd
from pyhive import hive

st.set_page_config(page_title="Travel Lakehouse Dashboard", layout="wide")


@st.cache_resource
def get_connection():
    return hive.Connection(
        host="localhost",
        port=10000,
        database="default"
    )


@st.cache_data(ttl=300)
def load_countries():
    query = """
        select distinct country_name
        from default.gold_city_full_profile
        order by country_name
    """
    return pd.read_sql(query, get_connection())


@st.cache_data(ttl=300)
def load_cities(country_name: str):
    query = f"""
        select distinct city_name
        from default.gold_city_full_profile
        where country_name = '{country_name}'
        order by city_name
    """
    return pd.read_sql(query, get_connection())


@st.cache_data(ttl=300)
def load_city_profile(country_name: str, city_name: str):
    query = f"""
        select *
        from default.gold_city_full_profile
        where country_name = '{country_name}'
          and city_name = '{city_name}'
    """
    return pd.read_sql(query, get_connection())


@st.cache_data(ttl=300)
def load_country_budget(country_name: str):
    query = f"""
        select *
        from default.gold_country_budget_summary
        where country_name = '{country_name}'
        order by budget_level
    """
    return pd.read_sql(query, get_connection())


@st.cache_data(ttl=300)
def load_country_city_summary(country_name: str):
    query = f"""
        select
            city_name,
            attraction_count,
            restaurant_count,
            local_food_count,
            tourism_score_raw,
            current_temp_c,
            current_condition_text,
            estimated_total_daily_cost
        from default.gold_city_full_profile
        where country_name = '{country_name}'
        order by city_name
    """
    return pd.read_sql(query, get_connection())


st.title("Travel Lakehouse Dashboard")
st.caption("Built on gold-layer datamarts from Spark + dbt + Airflow + MinIO")

try:
    countries_df = load_countries()
except Exception as e:
    st.error("Could not connect to Spark Thrift or load gold tables.")
    st.code(str(e))
    st.stop()

if countries_df.empty:
    st.warning("No data found in gold_city_full_profile.")
    st.stop()

col1, col2 = st.columns(2)

with col1:
    selected_country = st.selectbox(
        "Select country",
        countries_df["country_name"].tolist()
    )

cities_df = load_cities(selected_country)

with col2:
    selected_city = st.selectbox(
        "Select city",
        cities_df["city_name"].tolist() if not cities_df.empty else []
    )

if not selected_city:
    st.warning("No city found for the selected country.")
    st.stop()

profile_df = load_city_profile(selected_country, selected_city)
budget_df = load_country_budget(selected_country)
summary_df = load_country_city_summary(selected_country)

if profile_df.empty:
    st.warning("No profile data found for the selected city.")
    st.stop()

row = profile_df.iloc[0]

st.subheader(f"{row['city_name']}, {row['country_name']}")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Tourism Score", row.get("tourism_score_raw", "N/A"))
m2.metric("Attractions", int(row.get("attraction_count", 0)))
m3.metric("Restaurants", int(row.get("restaurant_count", 0)))
m4.metric("Local Foods", int(row.get("local_food_count", 0)))

left, right = st.columns([1.2, 1])

with left:
    st.markdown("### City Overview")
    st.write(f"**Local Name:** {row.get('city_name_local', 'N/A')}")
    st.write(f"**Capital City:** {row.get('capital_city', 'N/A')}")
    st.write(f"**Population:** {row.get('population', 'N/A')}")
    st.write(f"**Language:** {row.get('language_main', 'N/A')}")
    st.write(f"**Currency:** {row.get('currency_code', 'N/A')} - {row.get('currency_name', 'N/A')}")
    st.write(f"**Safety Score:** {row.get('avg_safety_score', 'N/A')}")
    st.write(f"**Visa Category:** {row.get('visa_requirement_category', 'N/A')}")
    st.write(f"**Coastal:** {row.get('coastal_flag', 'N/A')} | **Mountain:** {row.get('mountain_flag', 'N/A')} | **Capital:** {row.get('is_capital', 'N/A')}")
    st.markdown("**Description**")
    st.write(row.get("city_description", "N/A"))

with right:
    st.markdown("### Latest Weather")
    st.write(f"**Condition:** {row.get('current_condition_text', 'N/A')}")
    st.write(f"**Temperature:** {row.get('current_temp_c', 'N/A')} °C")
    st.write(f"**Feels Like:** {row.get('current_feelslike_c', 'N/A')} °C")
    st.write(f"**Humidity:** {row.get('current_humidity', 'N/A')}%")
    st.write(f"**Wind:** {row.get('current_wind_kph', 'N/A')} kph")
    st.write(f"**Rain Chance:** {row.get('daily_chance_of_rain', 'N/A')}%")
    st.write(f"**Forecast Avg Temp:** {row.get('forecast_avgtemp_c', 'N/A')} °C")
    st.write(f"**Forecast Date:** {row.get('forecast_date', 'N/A')}")
    st.write(f"**Sunrise / Sunset:** {row.get('sunrise', 'N/A')} / {row.get('sunset', 'N/A')}")

a1, a2 = st.columns(2)

with a1:
    st.markdown("### Attraction Insights")
    st.write(f"**Average Attraction Rating:** {row.get('avg_attraction_rating', 'N/A')}")
    st.write(f"**Total Attraction Reviews:** {row.get('total_attraction_reviews', 'N/A')}")
    st.write(f"**Paid Attractions:** {row.get('paid_attraction_count', 'N/A')}")
    st.write(f"**Family Friendly Attractions:** {row.get('family_friendly_attraction_count', 'N/A')}")

with a2:
    st.markdown("### Food Insights")
    st.write(f"**Country Food Count:** {row.get('total_food_count', 'N/A')}")
    st.write(f"**Average Food Popularity:** {row.get('avg_popularity_score', 'N/A')}")
    st.write(f"**Average Food Price:** {row.get('avg_price', 'N/A')}")
    st.write(f"**Vegetarian Foods:** {row.get('vegetarian_count', 'N/A')}")
    st.write(f"**Spicy Foods:** {row.get('spicy_count', 'N/A')}")

st.markdown("### Country Budget Summary")
if budget_df.empty:
    st.info("No budget data found.")
else:
    st.dataframe(budget_df, use_container_width=True)

st.markdown("### Country City Comparison")
if summary_df.empty:
    st.info("No comparison data found.")
else:
    st.dataframe(summary_df, use_container_width=True)

st.caption("Data source: gold datamarts generated with dbt models on Spark Thrift")