import os
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def sanitize_name(value: str) -> str:
    return(
        str(value)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )

def fetch_weather(api_key: str, city: str, days: int = 7) -> dict:
    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {
        "key": api_key,
        "q": city,
        "days": days,
        "aqi": "no",
        "alerts": "no"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    load_dotenv()

    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError("WEATHER_API_KEY not found in environment variables")
    
    countries_path = Path("data/raw/static/countries.csv")
    if not countries_path.exists():
        raise FileNotFoundError(f"{countries_path} not found")
    
    df_countries = pd.read_csv(countries_path , encoding="latin1",
    engine="python")


    required_column = {"country_name", "capital_city", "country_code"}
    missing_cols = [col for col in required_column if col not in df_countries.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in countries.csv: {', '.join(missing_cols)}")
    
    extraction_date = datetime.utcnow().strftime("%Y-%m-%d")
    extraction_ts = datetime.utcnow().isoformat()

    output_base = Path("data/raw/api/weather") / f"extraction_date={extraction_date}"
    ensure_directory(output_base)

    success_count = 0
    failed_requests = []

    for _, row in df_countries.iterrows():
        country_name = row["country_name"]
        capital_city = row["capital_city"]
        country_code = row["country_code"]

        if pd.isna(capital_city) or pd.isna(country_code):
            print(f"Skipping {country_name} due to missing capital city or country code")
            continue

        safe_country = sanitize_name(country_name)
        safe_city = sanitize_name(capital_city)

        country_dir = output_base / f"country={safe_country}" / f"city={safe_city}"
        ensure_directory(country_dir)


        try:
            weather_data = fetch_weather(
                api_key = api_key,
                city = str(capital_city).strip(),
                days = 7
            )

            enriched_payload = {
                "metadata": {
                    "source": "weatherapi.com",
                    "extraction_date": extraction_date,
                    "extraction_timestamp": extraction_ts,
                    "country_name": None if pd.isna(country_name) else country_name,
                    "capital_city": None if pd.isna(capital_city) else capital_city,
                    "country_code": None if pd.isna(country_code) else country_code,
                    "requested_city": str(capital_city).strip()

                },
                "response": weather_data
            }

            output_file = country_dir / "response.json"
            with open(output_file, "w",encoding="latin1") as f:
                json.dump(enriched_payload, f, ensure_ascii=False, indent=4)    

            success_count += 1
            print(f"Successfully fetched weather for {capital_city}, {country_name} ({country_code})")

        except requests.exceptions.RequestException as e:
            failed_requests.append({
                "country_name": country_name,
                "capital_city": capital_city,
                "country_code": country_code,
                "error": str(e)
            })
            print(f"Failed to fetch weather for {capital_city}, {country_name} ({country_code}): {e}")

    
    print("\n=== FETCH SUMMARY ===")
    print(f"Successful fetches: {success_count}")
    print(f"Failed fetches: {len(failed_requests)}")

    if failed_requests:
        failed_path = output_base / "failed_requests.json"
        with open(failed_path, "w", encoding="latin1") as f:
            json.dump(failed_requests, f, ensure_ascii=False, indent=4)
        print(f"Details of failed requests saved to {failed_path}")


if __name__ == "__main__":
    main()