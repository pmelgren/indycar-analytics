import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# Set up GCS
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "dbt-service-account-credentials.json"))
)
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket("motorstats-clean-pq")


def concat_prefix(gcs_prefix, output_path):
    """Download all .pq blobs under gcs_prefix, concatenate, and upload to output_path."""
    blobs = list(bucket.list_blobs(prefix=gcs_prefix))
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.pq'):
            df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
            dfs.append(df)
        else:
            print(f"Skipping non-parquet file: {blob.name}")

    if not dfs:
        print(f"No parquet files found under {gcs_prefix}")
        return 

    dfall = pd.concat(dfs).reset_index(drop=True)
    object_cols = dfall.select_dtypes(include=["object"]).columns
    if len(object_cols):
        dfall[object_cols] = dfall[object_cols].apply(lambda col: col.astype("string[python]"))
    dfall.columns = [c.replace('.', '_') for c in dfall.columns]

    print(f"{gcs_prefix} -> {output_path}  rows={len(dfall)}  cols={list(dfall.columns)}")
    bucket.blob(output_path).upload_from_string(
        data=dfall.to_parquet(index=False), content_type="application/octet-stream"
    )

if __name__ == '__main__':
    for report_type, source_type, session_type in [
        ("results","PDF", "Race"),
        ("results","PDF", "Qualifying"),
        ("results","PDF", "Practice"),
        ("results","HTML", "Race"),
        ("results","HTML", "Qualifying"),
        ("results","HTML", "Practice"),
        ("lapcharts","","Race")
    ]:
        concat_prefix(
            gcs_prefix="/".join([x for x in (report_type,source_type,session_type) if x != ""]),
            output_path=f"{report_type}/combined_{source_type if source_type != '' else report_type}_{session_type}.pq",
        )
