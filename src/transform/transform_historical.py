import os
import json
import pandas as pd
from datetime import datetime, timezone
from src.utils.gcp_utils import upload_to_gcs

# ---------- CONFIG ----------
BUCKET_NAME = "etl-projet"
RAW_PATH = "data/raw"
CLEAN_PATH = "data/clean"
# ----------------------------

def load_json(source_prefix):
    """Charge le dernier fichier JSON brut pour une source donnée (historique)."""
    files = [f for f in os.listdir(RAW_PATH) if f.startswith(source_prefix) and f.endswith(".json")]
    if not files:
        raise FileNotFoundError(f"Aucun fichier trouvé pour {source_prefix}")
    
    latest_file = sorted(files)[-1]
    path = os.path.join(RAW_PATH, latest_file)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"📥 Chargé : {latest_file}")
    return data, latest_file


# ---------- TRANSFORMATION MÉTÉO ----------
def transform_weather_history(data):
    """Transforme les données météo journalières en DataFrame."""
    daily = data.get("daily", {})
    df = pd.DataFrame({
        "date": daily.get("time"),
        "temp_max": daily.get("temperature_2m_max"),
        "temp_min": daily.get("temperature_2m_min"),
        "precipitation": daily.get("precipitation_sum")
    })
    df["date"] = pd.to_datetime(df["date"])
    print(f"🌤 Météo : {len(df)} jours chargés")
    return df


# ---------- TRANSFORMATION AIR ----------
def transform_air_history(data):
    """Transforme les données de qualité de l'air horaires en moyennes journalières."""
    hourly = data.get("hourly", {})
    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date

    # Moyenne journalière des polluants
    daily_air = (
        df.groupby("date")
        .agg({
            "pm10": "mean",
            "pm2_5": "mean",
            "nitrogen_dioxide": "mean",
            "ozone": "mean",
            "sulphur_dioxide": "mean",
            "carbon_monoxide": "mean",
            "european_aqi": "mean"
        })
        .reset_index()
    )
    daily_air["date"] = pd.to_datetime(daily_air["date"])
    print(f"💨 Air : {len(daily_air)} jours agrégés")
    return daily_air


# ---------- FUSION ----------
def merge_air_weather(air_df, weather_df):
    """Fusionne les moyennes de pollution et les données météo sur la date."""
    merged = pd.merge(air_df, weather_df, on="date", how="inner")
    print(f"🔗 Fusion : {len(merged)} jours communs")
    return merged


# ---------- SAUVEGARDE ----------
def save_and_upload(df):
    """Sauvegarde le DataFrame en JSON et envoie dans GCS."""
    os.makedirs(CLEAN_PATH, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    file_name = f"historical_2024_2025_{timestamp}.json"
    local_path = os.path.join(CLEAN_PATH, file_name)

    df.to_json(local_path, orient="records", lines=True, date_format="iso")
    print(f"✅ Fichier nettoyé enregistré : {local_path}")

    gcs_path = f"clean/{file_name}"
    upload_to_gcs(BUCKET_NAME, local_path, gcs_path)
    print(f"✅ Fichier {file_name} envoyé dans GCS.")


# ---------- PIPELINE COMPLET ----------
def run_transform_historical():
    """Pipeline complet : lecture, transformation, fusion, sauvegarde."""
    print("⚙️ Début de la transformation historique Open-Meteo...")

    air_data, _ = load_json("openmeteo_history_air")
    weather_data, _ = load_json("openmeteo_history_weather")

    air_df = transform_air_history(air_data)
    weather_df = transform_weather_history(weather_data)
    merged_df = merge_air_weather(air_df, weather_df)

    print("📊 Exemple de lignes fusionnées :")
    print(merged_df.head())

    save_and_upload(merged_df)
    print("🎯 Transformation historique terminée avec succès !")


if __name__ == "__main__":
    run_transform_historical()
