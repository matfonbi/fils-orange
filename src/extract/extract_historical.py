import os
import json
import requests
from datetime import datetime
from src.utils.gcp_utils import upload_to_gcs

# ---------- CONFIG ----------
BUCKET_NAME = "etl-projet"
LAT, LON = 48.8566, 2.3522  # Paris
START_DATE = "2024-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
RAW_FOLDER = "raw/"
# ----------------------------


def fetch_openmeteo_weather_history(lat, lon, start_date, end_date):
    """Récupère les données météo journalières historiques."""
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&timezone=Europe%2FParis"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    print("🌤 Météo historique récupérée avec succès.")
    return r.json()


def fetch_openmeteo_air_history(lat, lon, start_date, end_date):
    """Récupère les données de qualité de l'air historiques (horaires)."""
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,european_aqi"
        "&timezone=Europe%2FParis"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    print("💨 Données qualité de l'air historiques récupérées avec succès.")
    return r.json()


def save_to_gcs(data, source):
    """Sauvegarde localement puis envoie sur GCS."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    os.makedirs("data/raw", exist_ok=True)
    file_name = f"{source}_{timestamp}.json"
    local_path = os.path.join("data", "raw", file_name)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    gcs_path = f"{RAW_FOLDER}{source}/{file_name}"
    upload_to_gcs(BUCKET_NAME, local_path, gcs_path)
    print(f"✅ Fichier {file_name} envoyé dans GCS.")


def run_extract_historical():
    """Exécute la récupération historique complète."""
    print(f"📦 Extraction historique Open-Meteo de {START_DATE} à {END_DATE} pour Paris...")

    try:
        weather_data = fetch_openmeteo_weather_history(LAT, LON, START_DATE, END_DATE)
        save_to_gcs(weather_data, "openmeteo_history_weather")

        air_data = fetch_openmeteo_air_history(LAT, LON, START_DATE, END_DATE)
        save_to_gcs(air_data, "openmeteo_history_air")

        print("🎯 Extraction historique terminée avec succès !")

    except Exception as e:
        print("❌ Erreur pendant l'extraction historique :", e)


if __name__ == "__main__":
    run_extract_historical()
