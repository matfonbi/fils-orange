from src.utils.gcp_utils import upload_to_gcs, list_gcs_files

bucket = "etl-projet"  # ⚠️ remplace ici
upload_to_gcs(bucket, "README.md", "test/README.md")

print(list_gcs_files(bucket, "test/"))
