import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# set up GCS
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbt-service-account-credentials.json"))
)
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket("motorstats-clean-pq")


def parse_and_clean_html_results(files):

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.upper() == 'ALL':
            files = os.listdir("html/results/")
        else:
            files = [files]

    for file in files:
        if file.split('.')[-1] != 'html':
            print(f'Skipping file {file}')
            continue

        parquetfile = file.replace('.html', '.pq')
        session_prefix = 'Qualifying' if 'QUAL' in file.upper() else 'Race' if 'RACE' in file.upper() else 'Practice'
        gcs_object_path = f"results/HTML/{session_prefix}/{parquetfile}"

        if bucket.blob(gcs_object_path).exists():
            print(f"Skipping existing GCS object: gs://motorstats-clean-pq/{gcs_object_path}")
            continue

        with open(os.path.join('html', 'results', file), 'r', encoding='utf-8') as f:
            table_html = f.read()

        df = pd.read_html(table_html, converters={'No.': str})[0]
        df['file'] = file

        blob = bucket.blob(gcs_object_path)
        blob.upload_from_string(data=df.to_parquet(index=False), content_type="application/octet-stream")
        print(f"Uploaded gs://motorstats-clean-pq/{gcs_object_path}")


if __name__ == '__main__':
    parse_and_clean_html_results('all')
