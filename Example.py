from loading._download_session_reports import download_session_reports
from parsing._section_results_main import parse_and_clean_section_results

# download all pdf for 2017
download_session_reports(2017,2017)

# parse info from the section results pdf
parse_and_clean_section_results(['sectionresults_2017-03-12_3651_Firestone Grand Prix of St. Petersburg.pdf'])

