import os
import json
import requests
from datetime import datetime, timezone
from src.utils.gcp_utils import upload_to_gcs

# ---------- CONFIG ----------
BUCKET_NAME = "etl-projet"   # ⚠️ ton vrai nom de bucket
CITY = "Paris"
LAT, LON = 48.8566, 2.3522   # coordonnées de Paris
RAW_FOLDER = "raw/"
# ----------------------------

from datetime import date, timedelta

def fetch_openmeteo_air(lat, lon):
    """Récupère les données de qualité de l'air pour la journée d'hier."""
    # Calcul de la date d’hier
    yesterday = date.today() - timedelta(days=1)
    start_date = end_date = yesterday.isoformat()

    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,european_aqi"
        "&timezone=Europe%2FParis"
    )

    print(f"📡 Requête Open-Meteo (veille) : {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()



def fetch_openmeteo_weather(lat, lon):
    """Récupère la météo journalière de la veille."""
    yesterday = date.today() - timedelta(days=1)
    start_date = end_date = yesterday.isoformat()

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,windspeed_10m_mean"
        "&timezone=Europe/Paris"
    )

    print(f"📡 Requête Open-Meteo (météo veille) : {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()



def save_local_and_upload(data, source):
    """Sauvegarde localement et envoie le fichier vers GCS."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    filename = f"{source}_{timestamp}.json"
    local_path = os.path.join("data", "raw", filename)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    gcs_path = f"{RAW_FOLDER}{source}/{filename}"
    upload_to_gcs(BUCKET_NAME, local_path, gcs_path)
    print(f"✅ Fichier {filename} envoyé dans GCS.")


def run_extract():
    """Exécute l’extraction : air quality + météo."""
    print(f"📦 Extraction des données Open-Meteo pour {CITY}…")

    try:
        # Qualité de l’air
        air_data = fetch_openmeteo_air(LAT, LON)
        save_local_and_upload(air_data, "openmeteo_air")
        print("✅ Données de qualité de l’air envoyées sur GCS")

        # Météo
        weather_data = fetch_openmeteo_weather(LAT, LON)
        save_local_and_upload(weather_data, "openmeteo_weather")
        print("✅ Données météo envoyées sur GCS")

        print("🎯 Extraction terminée avec succès !")

    except Exception as e:
        print("❌ Erreur pendant l'extraction :", e)


if __name__ == "__main__":
    run_extract()

