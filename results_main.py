import os
from parsing._cleaning import parse_results_pdf, clean_results_df
   
def parse_and_clean_results(files):

    # if files is 'All', get the list of all files
    if type(files) == str:
        if files.upper() == 'ALL':
            files = os.listdir("pdfs/results/")
        else:
            files = [files]
            
    for file in files:
        
        # skip exhibition race
        if file == 'results_2024-03-24_6345_$1 Million Challenge.pdf':
            continue
        
        if file.split('.')[-1] != 'pdf':
            print(f'Skipping file {file}')
            continue
            
        # read and clean the main results table from each file than save as pq
        parquetfile = file.replace('.pdf', '.pq')
        parquet_path = os.path.join('cleandata', 'results', parquetfile)
        
        if parquetfile not in os.listdir(os.path.join('cleandata', 'results')):
            
            df = parse_results_pdf(os.path.join('pdfs','results',file))
            dfclean = clean_results_df(df)
            dfclean.to_parquet(parquet_path)
