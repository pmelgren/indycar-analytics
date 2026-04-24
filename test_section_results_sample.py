"""
Test script to run parse_and_clean_section_results on a random sample of files.
Picks 8 random files per year.
"""
import os
import re
import random
import warnings
from collections import defaultdict
from parsing._section_results_main import parse_and_clean_section_results

warnings.filterwarnings('ignore', category=FutureWarning)


if __name__ == '__main__':
    pdf_dir = 'pdfs/sectionresults'
    
    # Ensure cleandata directory exists
    os.makedirs('cleandata/sectionresults', exist_ok=True)
    
    # Get all files and group by year
    all_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    files_by_year = defaultdict(list)
    
    for f in all_files:
        year_match = re.search(r'(19|20)\d{2}', f)
        if year_match:
            year = year_match.group()
            files_by_year[year].append(f)
    
    # Sample 8 files per year
    sample_files = []
    for year in sorted(files_by_year.keys()):
        year_files = files_by_year[year]
        num_to_sample = min(8, len(year_files))
        sampled = random.sample(year_files, num_to_sample)
        sample_files.extend(sampled)
        print(f"{year}: {num_to_sample} files sampled")
    
    print(f"\nRunning parse_and_clean_section_results on {len(sample_files)} files")
    
    parse_and_clean_section_results(sample_files)
    

