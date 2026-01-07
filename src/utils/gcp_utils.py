# src/utils/gcp_utils.py
import os
from google.cloud import storage, bigquery

PROJECT_ID = os.getenv("PROJECT_ID", "")  # optionnel, peut être vide en cloud

def get_gcs_client():
    # Auth implicite (Cloud Run / ADC en local)
    return storage.Client(project=PROJECT_ID or None)

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID or None)

def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str) -> None:
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"✅ Fichier {source_file} envoyé dans gs://{bucket_name}/{destination_blob}")
