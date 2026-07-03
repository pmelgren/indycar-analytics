import os
import fitz
from google.cloud import storage
from google.oauth2 import service_account
from .parse_lap_charts import parse_lap_chart_file

# set up GCS
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "dbt-service-account-credentials.json"))
)
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket("motorstats-clean-pq")


def parse_and_clean_lap_charts(files):

    if type(files) == str:
        if files.lower() == 'all':
            files = os.listdir("pdfs/lap charts/")
        else:
            files = [files]

    for file in files:
        if file.split('.')[-1] != 'pdf':
            print(f'Skipping file {file}')
            continue

        parquetfile = file.replace('.pdf', '.pq')
        gcs_object_path = f"lapcharts/{parquetfile}"

        if bucket.blob(gcs_object_path).exists():
            print(f"Skipping existing GCS object: gs://motorstats-clean-pq/{gcs_object_path}")
            continue

        doc = fitz.open(os.path.join('pdfs', 'lap charts', file))
        df = parse_lap_chart_file(doc)

        if df.empty:
            print(f"No lap chart rows parsed for {file}")
            continue

        df['file'] = file

        blob = bucket.blob(gcs_object_path)
        blob.upload_from_string(data=df.to_parquet(index=False), content_type="application/octet-stream")
        print(f"Uploaded gs://motorstats-clean-pq/{gcs_object_path}")
