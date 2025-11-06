from src.extract.extract_data import run_extract
from src.transform.transform_data import run_transform
from src.load.load_data import load_json_to_bigquery

def run_etl(request):
    try:
        print("🚀 Démarrage du pipeline Open-Meteo...")
        run_extract()
        run_transform()
        load_json_to_bigquery("air_quality", "historical_data", "data/clean/merged_latest.json")
        print("✅ Pipeline terminé avec succès !")
        return "ETL exécuté avec succès", 200
    except Exception as e:
        print("❌ Erreur :", e)
        return f"Erreur : {e}", 500
