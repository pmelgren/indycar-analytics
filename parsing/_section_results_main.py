import fitz
import json
import os
import pandas as pd
import logging
from ._util import parse_file
from ._cleaning import clean_section_results_page, parse_sections_table
import time
from datetime import datetime

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

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.lower() == 'all':
            files = os.listdir("pdfs/sectionresults/")
        
    for file in files:
        jsonfile = file.replace('.pdf', '.json')
        json_path = os.path.join('parsedraw', 'sectionresults', jsonfile)
        parquetfile = file.replace('.pdf', '.pq')
        parquet_path = os.path.join('cleandata', 'sectionresults', parquetfile)
        
        json_success = False
        parquet_success = False
        
        # Step 1: parse the pdf file into a json
        try:
            if jsonfile not in os.listdir(os.path.join('parsedraw', 'sectionresults')):
                start = time.perf_counter()
                logger.debug(f'Parsing {file}')
                doc = fitz.open(os.path.join('pdfs', 'sectionresults', file))
                allrows = parse_file(doc)
        
                with open(json_path, "w") as f:
                    json.dump(allrows, f)
                    
                logger.debug(f'PDF->JSON time: {time.perf_counter() - start:.2f}s')
            json_success = True
        except Exception as e:
            logger.warning(f'FAILED PDF->JSON: {file} | {e}')
        
        # Step 2: clean the json into a parquet file
        if json_success:
            try:
                if parquetfile not in os.listdir(os.path.join('cleandata', 'sectionresults')):
                    logger.debug(f'Cleaning {jsonfile}')
                    df = pd.read_json(json_path)
                    
                    st = parse_sections_table(df)
                    
                    dfps = []
                    for p in df.page.unique():
                        dfp = df.loc[df.page == p].copy()
                        page_result = clean_section_results_page(dfp, st)
                        if page_result.empty:
                            logger.debug(f'Skipping page {p} in {file} - does not contain Section Data')
                        else:
                            dfps.append(page_result)
                        
                    dfclean = pd.concat(dfps)
                    dfclean.to_parquet(parquet_path)
                parquet_success = True
            except Exception as e:
                logger.warning(f'FAILED JSON->Parquet: {file} | {e}')
        
        # Log success if both steps completed
        if json_success and parquet_success:
            logger.info(f'SUCCESS: {file}')
