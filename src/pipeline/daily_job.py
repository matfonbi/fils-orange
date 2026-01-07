# src/pipeline/daily_job.py
import os
import requests
from datetime import date, timedelta
from google.cloud import bigquery
from src.utils.gcp_utils import get_bq_client

PROJECT_ID = os.getenv("PROJECT_ID", "fils-orange")  # mets ton project id si tu veux
DATASET_ID = os.getenv("BQ_DATASET", "air_quality")
TABLE_ID = os.getenv("BQ_TABLE", "historical_data")
CITY = os.getenv("CITY", "Paris")
LAT = float(os.getenv("LAT", "48.8566"))
LON = float(os.getenv("LON", "2.3522"))

def _yesterday_iso() -> str:
    return (date.today() - timedelta(days=1)).isoformat()

def fetch_air_daily_yesterday(lat: float, lon: float) -> dict:
    d = _yesterday_iso()
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={d}&end_date={d}"
        "&daily=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,european_aqi"
        "&timezone=Europe%2FParis"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_weather_daily_yesterday(lat: float, lon: float) -> dict:
    d = _yesterday_iso()
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={d}&end_date={d}"
        "&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,windspeed_10m_mean"
        "&timezone=Europe%2FParis"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def build_row(city: str, air_json: dict, weather_json: dict) -> dict:
    # air.daily et weather.daily ont 1 valeur (la veille)
    air = air_json["daily"]
    w = weather_json["daily"]

    d_air = air["time"][0]
    d_w = w["time"][0]
    if d_air != d_w:
        raise ValueError(f"Dates mismatch air={d_air} weather={d_w}")

    return {
        "city": city,
        "date": d_air,  # string YYYY-MM-DD (BigQuery DATE ok si autodetect ou schema)
        "pm10": air["pm10"][0],
        "pm2_5": air["pm2_5"][0],
        "carbon_monoxide": air["carbon_monoxide"][0],
        "nitrogen_dioxide": air["nitrogen_dioxide"][0],
        "ozone": air["ozone"][0],
        "sulphur_dioxide": air["sulphur_dioxide"][0],
        "european_aqi": air["european_aqi"][0],
        "temp_max": w["temperature_2m_max"][0],
        "temp_min": w["temperature_2m_min"][0],
        "temp_mean": w["temperature_2m_mean"][0],
        "precipitation": w["precipitation_sum"][0],
        "windspeed_mean": w["windspeed_10m_mean"][0],
    }

def ensure_staging_table(client: bigquery.Client, dataset_id: str) -> str:
    staging_table = f"{PROJECT_ID}.{dataset_id}.staging_daily"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{staging_table}` (
      city STRING,
      date DATE,
      pm10 FLOAT64,
      pm2_5 FLOAT64,
      carbon_monoxide FLOAT64,
      nitrogen_dioxide FLOAT64,
      ozone FLOAT64,
      sulphur_dioxide FLOAT64,
      european_aqi FLOAT64,
      temp_max FLOAT64,
      temp_min FLOAT64,
      temp_mean FLOAT64,
      precipitation FLOAT64,
      windspeed_mean FLOAT64
    )
    """
    client.query(ddl).result()
    return staging_table

def truncate_staging(client: bigquery.Client, staging_table: str) -> None:
    client.query(f"TRUNCATE TABLE `{staging_table}`").result()

def load_row_to_staging(client: bigquery.Client, staging_table: str, row: dict) -> None:
    # insert_rows_json attend des types Python; pour DATE, on peut passer "YYYY-MM-DD"
    errors = client.insert_rows_json(staging_table, [row])
    if errors:
        raise RuntimeError(f"Insert staging errors: {errors}")

def merge_into_history(client: bigquery.Client, dataset_id: str, table_id: str, staging_table: str) -> None:
    target = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    sql = f"""
    MERGE `{target}` T
    USING `{staging_table}` S
    ON T.date = S.date AND T.city = S.city
    WHEN MATCHED THEN UPDATE SET
      pm10 = S.pm10,
      pm2_5 = S.pm2_5,
      carbon_monoxide = S.carbon_monoxide,
      nitrogen_dioxide = S.nitrogen_dioxide,
      ozone = S.ozone,
      sulphur_dioxide = S.sulphur_dioxide,
      european_aqi = S.european_aqi,
      temp_max = S.temp_max,
      temp_min = S.temp_min,
      temp_mean = S.temp_mean,
      precipitation = S.precipitation,
      windspeed_mean = S.windspeed_mean
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    client.query(sql).result()

def main():
    print("🚀 Daily job: récupération de la veille…")
    air = fetch_air_daily_yesterday(LAT, LON)
    weather = fetch_weather_daily_yesterday(LAT, LON)
    row = build_row(CITY, air, weather)
    print("✅ Ligne daily:", row)

    client = get_bq_client()
    staging = ensure_staging_table(client, DATASET_ID)
    truncate_staging(client, staging)
    load_row_to_staging(client, staging, row)
    merge_into_history(client, DATASET_ID, TABLE_ID, staging)

    print(f"✅ Upsert OK dans {DATASET_ID}.{TABLE_ID} pour {row['date']} ({row['city']})")

if __name__ == "__main__":
    main()
