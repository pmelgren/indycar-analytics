import fitz
import os
import pandas as pd
import logging
from indycar_analytics.util.pdf_utils import parse_file
from .cleaning import clean_section_results_page, parse_sections_table
import time
from datetime import datetime
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

# Set up logger
suffix = datetime.now().strftime('%Y%m%d%H%M%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# File handler - captures all DEBUG and above
file_handler = logging.FileHandler(f'./logs/section_parser-{suffix}.log', mode='a')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Stream handler - captures INFO and above (console output)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
   
def parse_and_clean_section_results(files):
    failed_files = []

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.lower() == 'all':
            files = os.listdir("pdfs/sectionresults/")
        
    for file in files:
        parquetfile = file.replace('.pdf', '.pq')
        gcs_object_path = f"sectionresults/{parquetfile}"

        try:
            if bucket.blob(gcs_object_path).exists():
                logger.info(f"Skipping existing GCS object: gs://motorstats-clean-pq/{gcs_object_path}")
                continue

            start = time.perf_counter()
            logger.debug(f'Parsing and cleaning {file}')
            doc = fitz.open(os.path.join('pdfs', 'sectionresults', file))
            allrows = parse_file(doc)
            df = pd.DataFrame(allrows)

            st = parse_sections_table(df)

            dfps = []
            for p in df.page.unique():
                dfp = df.loc[df.page == p].copy()
                page_result = clean_section_results_page(dfp, st)
                if page_result.empty:
                    logger.debug(f'Skipping page {p} in {file} - does not contain Section Data')
                else:
                    dfps.append(page_result)

            if not dfps:
                logger.warning(f'No section result rows parsed for {file}')
                continue

            dfclean = pd.concat(dfps)
            blob = bucket.blob(gcs_object_path)
            blob.upload_from_string(data=dfclean.to_parquet(index=False), content_type="application/octet-stream")
            logger.debug(f'PDF->Parquet time: {time.perf_counter() - start:.2f}s')
            logger.info(f'SUCCESS: {file}')
        except Exception as e:
            failed_files.append(file)
            logger.warning(f'FAILED PDF->Parquet: {file} | {e}')

    if failed_files:
        logger.info('Failed files:')
        for f in failed_files:
            logger.info(f'- {f}')
