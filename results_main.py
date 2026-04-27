import os
from parsing._cleaning import parse_results_pdf, clean_results_df
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

def parse_and_clean_results(files):

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.upper() == 'ALL':
            files = os.listdir("pdfs/results/")
        else:
            files = [files]

    for file in files:
        # skip exhibition race and some unusable PDFs
        if (('$1 Million Challenge.pdf' in file) or 
            ('20130524_2705_Indianapolis_500_PRACTICE_10_results.pdf' in file)
            or ('3528_KOHLER_Grand_Prix_PRACTICE_FINAL' in file)):
            continue
        
        if file.split('.')[-1] != 'pdf':
            print(f'Skipping file {file}')
            continue
            
        # read and clean the main results table from each file than save as pq
        parquetfile = file.replace('.pdf', '.pq')
        session_prefix = 'Qualifying' if 'QUALI' in file else 'Race' if 'RACE' in file else 'Practice'
        gcs_object_path = f"results/{session_prefix}/{parquetfile}"

        if bucket.blob(gcs_object_path).exists():
            print(f"Skipping existing GCS object: gs://motorstats-clean-pq/{gcs_object_path}")
            continue

        # parse pdf and clean resulting df
        df = parse_results_pdf(os.path.join('pdfs','results',file))
        dfclean = clean_results_df(df)
        dfclean['file'] = file

        # upload to GCS
        blob = bucket.blob(gcs_object_path)
        blob.upload_from_string(data=dfclean.to_parquet(index=False),content_type="application/octet-stream")
        print(f"Uploaded gs://motorstats-clean-pq/{gcs_object_path}")

if __name__ == '__main__':
    parse_and_clean_results('all')