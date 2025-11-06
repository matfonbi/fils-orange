from src.extract.extract_data import run_extract
from src.extract.extract_historical import run_extract_historical
from src.transform.transform_data import run_transform
from src.transform.transform_historical import run_transform_historical

def run_etl(request):
    """Pipeline complet exécuté dans Cloud Functions."""
    try:
        print("🚀 Démarrage du pipeline Open-Meteo...")
        run_extract()
        run_transform()
        run_extract_historical()
        run_transform_historical()
        print("✅ Pipeline terminé avec succès !")
        return "ETL exécuté avec succès", 200
    except Exception as e:
        print("❌ Erreur :", e)
        return f"Erreur : {e}", 500

