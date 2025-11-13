import os
import logging
from ._cleaning import clean_results_pdf
from datetime import datetime

# Set up logger
suffix = datetime.now().strftime('%Y%m%d%H%M%S')
logging.basicConfig(
    filename=f'./logs/results_parser-{suffix}.log',
    filemode='a',  # append mode
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
   
def parse_and_clean_results(files):

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.upper() == 'ALL':
            files = os.listdir("pdfs/results/")
        
    for file in files:
        try:
            
            # read and clean the main results table from each file than save as pq
            parquetfile = file.replace('.pdf', '.pq')
            parquet_path = os.path.join('cleandata', 'results', parquetfile)
            
            if parquetfile not in os.listdir(os.path.join('cleandata', 'results')):
                logger.info(f'Parsing and cleaning {file}')
                
                dfclean = clean_results_pdf(os.path.join('pdfs','results',file))
                dfclean.to_parquet(parquet_path)
                
                logger.info(f'Successfully parsed and cleaned {file}')
                
        except Exception as e:
            logger.warning(f'Failed to parse and clean {file} with error: {e}')
