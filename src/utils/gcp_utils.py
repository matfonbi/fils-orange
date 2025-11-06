import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv

# Charger les variables d'environnement (.env)
load_dotenv()

#PROJECT_ID = os.getenv("PROJECT_ID")
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/etl-service-account.json"
PROJECT_ID = "fils-orange"



# --- Cloud Storage ---
def get_gcs_client():
    return storage.Client(project=PROJECT_ID)


def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Upload un fichier local vers GCS."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"✅ Fichier {source_file} envoyé dans gs://{bucket_name}/{destination_blob}")


def list_gcs_files(bucket_name, prefix=""):
    """Liste les fichiers d'un dossier GCS."""
    client = get_gcs_client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]


# --- BigQuery ---
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def load_to_bigquery(dataset_id, table_id, gcs_uri):
    """Charge un fichier depuis GCS vers une table BigQuery."""
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"✅ Données chargées dans BigQuery : {table_ref}")

