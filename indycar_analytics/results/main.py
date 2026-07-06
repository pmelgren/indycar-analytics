import os
from .cleaning import parse_results_pdf, clean_results_df
from google.cloud import storage
from google.oauth2 import service_account

# set up GCS
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "dbt-service-account-credentials.json"))
)
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket("motorstats-clean-pq")


def get_session_prefix(file_name):
    tokens = [p.upper() for p in file_name.replace('.pdf', '').split(';') if p]
    session_token = tokens[3] if len(tokens) > 3 else (tokens[2] if len(tokens) > 2 else '')
    if 'QUAL' in session_token or 'FAST_12' in session_token or 'FAST_6' in session_token:
        return 'Qualifying'
    if 'PRACTICE' in session_token or 'WARMUP' in session_token:
        return 'Practice'
    return 'Race'

def parse_and_clean_results(files):
    failed_files = []

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.upper() == 'ALL':
            files = os.listdir("pdfs/results/")
        else:
            files = [files]

    for file in files:
        try:
            # skip exhibition race and some unusable PDFs
            indy_500_bad = '20130524;2705;Indianapolis_500;PRACTICE_10;results.pdf'
            kohler_bad = '3528;KOHLER_Grand_Prix;PRACTICE_FINAL'
            if (('$1 Million Challenge.pdf' in file) or 
                (indy_500_bad in file) or (kohler_bad in file)):
                continue
            
            if file.split('.')[-1] != 'pdf':
                print(f'Skipping file {file}')
                continue
                
            # read and clean the main results table from each file than save as pq
            parquetfile = file.replace('.pdf', '.pq')
            session_prefix = get_session_prefix(file)
            gcs_object_path = f"results/PDF/{session_prefix}/{parquetfile}"

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
        except Exception as e:
            failed_files.append(file)
            print(f"FAILED {file}: {e}")

    if failed_files:
        print("\nFailed files:")
        for f in failed_files:
            print(f"- {f}")

if __name__ == '_ _main__':
    parse_and_clean_results('all')