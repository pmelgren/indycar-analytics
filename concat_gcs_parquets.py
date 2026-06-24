import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# Set up GCS
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbt-service-account-credentials.json"))
)
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket("motorstats-clean-pq")

# List all blobs in the results/ directory
for source_type, session_type in [
    # ("PDF", "Race"),
    # ("PDF", "Qualifying"),
    # ("PDF", "Practice"),
    ("HTML", "Race"),
    ("HTML", "Qualifying"),
    ("HTML", "Practice"),
]:
    blobs = bucket.list_blobs(prefix=f"results/{source_type}/{session_type}/")
    
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.pq'):
            content = blob.download_as_bytes()
            df = pd.read_parquet(BytesIO(content))
            dfs.append(df)
        else:
            print(f"Skipping non-parquet file: {blob.name}")

    if not dfs:
        print(f"No parquet files found under results/{source_type}/{session_type}/")
        continue

    dfall = pd.concat(dfs).reset_index(drop=True)
    object_cols = dfall.select_dtypes(include=["object"]).columns
    if len(object_cols):
        dfall[object_cols] = dfall[object_cols].apply(lambda col: col.astype("string[python]"))

    # make col names safe for bigquery
    dfall.columns = [c.replace('.','_') for c in dfall.columns]
    
    print(f'{source_type}/{session_type} columns: {dfall.columns}')
    gcs_path = f"results/combined_{source_type}_{session_type}.pq"
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(data=dfall.to_parquet(index=False), content_type="application/octet-stream")

print("\nDone!")
