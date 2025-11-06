import os
import json
import pandas as pd
from datetime import datetime, timezone
from src.utils.gcp_utils import upload_to_gcs

# ---------- CONFIG ----------
BUCKET_NAME = "etl-projet"   # ⚠️ ton vrai bucket
RAW_PATH = "data/raw"
CLEAN_PATH = "data/clean"
# ----------------------------


def load_json_from_local(source):
    """Charge le dernier fichier JSON brut depuis data/raw."""
    files = [f for f in os.listdir(RAW_PATH) if f.startswith(source) and f.endswith(".json")]
    if not files:
        raise FileNotFoundError(f"Aucun fichier trouvé pour la source {source}")
    
    latest_file = sorted(files)[-1]
    path = os.path.join(RAW_PATH, latest_file)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"📥 Chargé : {latest_file}")
    return data, latest_file


# ---------- TRANSFORM ----------
def transform_air(data):
    """Transforme les données de qualité de l'air Open-Meteo."""
    current = data.get("current", {})
    result = {
        "timestamp": current.get("time"),
        "pm25": current.get("pm2_5"),
        "pm10": current.get("pm10"),
        "co": current.get("carbon_monoxide"),
        "no2": current.get("nitrogen_dioxide"),
        "o3": current.get("ozone"),
        "so2": current.get("sulphur_dioxide"),
        "aqi_eu": current.get("european_aqi"),
    }
    return pd.DataFrame([result])


def transform_weather(data):
    """Transforme les données météo actuelles Open-Meteo."""
    current = data.get("current_weather", {})
    result = {
        "timestamp": current.get("time"),
        "temperature": current.get("temperature"),
        "windspeed": current.get("windspeed"),
        "winddirection": current.get("winddirection"),
        "weathercode": current.get("weathercode"),
    }
    return pd.DataFrame([result])


def merge_data(air_df, weather_df):
    """Fusionne les deux jeux de données sur le timestamp (UTC)."""
    air_df["timestamp"] = pd.to_datetime(air_df["timestamp"], utc=True)
    weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"], utc=True)

    # Si les timestamps sont légèrement différents, on garde le plus proche
    merged = pd.merge_asof(
        air_df.sort_values("timestamp"),
        weather_df.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("6h")  # tolérance large (6 heures)
    )
    return merged


# ---------- SAVE ----------
def save_and_upload(df):
    """Sauvegarde le DataFrame en JSON et l'envoie sur GCS."""
    os.makedirs(CLEAN_PATH, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    file_name = f"merged_{timestamp}.json"
    local_path = os.path.join(CLEAN_PATH, file_name)

    df.to_json(local_path, orient="records", indent=2, date_format="iso")
    print(f"✅ Fichier nettoyé enregistré : {local_path}")

    gcs_path = f"clean/{file_name}"
    upload_to_gcs(BUCKET_NAME, local_path, gcs_path)
    print(f"✅ Fichier {file_name} envoyé dans GCS.")


# ---------- MAIN ----------
def run_transform():
    """Étape complète : lecture, transformation, fusion, sauvegarde."""
    print("⚙️ Début de la transformation Open-Meteo...")

    air_data, _ = load_json_from_local("openmeteo_air")
    weather_data, _ = load_json_from_local("openmeteo_weather")

    air_df = transform_air(air_data)
    weather_df = transform_weather(weather_data)
    merged_df = merge_data(air_df, weather_df)

    print("📊 Données fusionnées :")
    print(merged_df.head())

    save_and_upload(merged_df)
    print("🎯 Transformation terminée avec succès !")


if __name__ == "__main__":
    run_transform()
