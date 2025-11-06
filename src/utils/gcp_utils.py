from google.cloud import storage, bigquery

# ✅ Google détectera automatiquement l'identité du service Cloud Run
# Pas besoin de clé ni d'os.environ

def get_gcs_client():
    """Retourne un client Cloud Storage authentifié automatiquement."""
    return storage.Client()

def get_bq_client():
    """Retourne un client BigQuery authentifié automatiquement."""
    return bigquery.Client()

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Envoie un fichier local vers un bucket GCS."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"✅ Fichier {source_file_name} envoyé dans gs://{bucket_name}/{destination_blob_name}")

def load_json_to_bigquery(dataset_id, table_id, json_file_path):
    """Charge un fichier JSON dans une table BigQuery."""
    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    with open(json_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()  # Attend la fin du job
    print(f"✅ Données chargées dans BigQuery → {dataset_id}.{table_id}")
