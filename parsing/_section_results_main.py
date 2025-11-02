import fitz
import json
import os
import pandas as pd
import logging
from ._parse_section_times import parse_section_results_file
from ._cleaning import clean_section_results_page, parse_sections_table
import time
from datetime import datetime

# Set up logger
suffix = datetime.now().strftime('%Y%m%d%H%M%S')
logging.basicConfig(
    filename=f'./logs/section_parser-{suffix}.log',
    filemode='a',  # append mode
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
   
def parse_and_clean_section_results(files):

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.lower() == 'ALL':
            files = os.listdir("pdfs/section results/")
        
    for file in files:
        try:
            # parse the pdf file into a json
            jsonfile = file.replace('.pdf', '.json')
            json_path = os.path.join('parsedraw', 'section results', jsonfile)
            
            if jsonfile not in os.listdir(os.path.join('parsedraw', 'section results')):
                
                start = time.perf_counter()
                logger.info(f'Parsing {file}')
                doc = fitz.open(os.path.join('pdfs', 'section results', file))
                allrows = parse_section_results_file(doc)
        
                with open(json_path, "w") as f:
                    json.dump(allrows, f)
                    
                logger.debug(f'Time taken: {time.perf_counter() - start}')
            
            # clean the json into a pq file in tabular form
            parquetfile = file.replace('.pdf', '.pq')
            parquet_path = os.path.join('cleandata', 'section results', parquetfile)
            
            if parquetfile not in os.listdir(os.path.join('cleandata', 'section results')):
                logger.info(f'Cleaning {jsonfile}')
                df = pd.read_json(json_path)
                
                st = parse_sections_table(df)
                
                dfps = []
                for p in df.page.unique():
                    dfp = df.loc[df.page == p].copy()
                    dfps.append(clean_section_results_page(dfp, st))
                    
                dfclean = pd.concat(dfps)
                dfclean.to_parquet(parquet_path)
                
                logger.info(f'Successfully parsed and cleaned {file}')
                
        except Exception as e:
            logger.warning(f'Failed to parse and clean {file} with error: {e}')
